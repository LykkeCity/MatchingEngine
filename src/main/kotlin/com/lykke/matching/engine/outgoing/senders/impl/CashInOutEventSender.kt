package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.senders.SpecializedCashInOutEventSender
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class CashInOutEventSender(val eventSender: List<SpecializedCashInOutEventSender>,
                           val cashInOutEventData: BlockingQueue<CashInOutEventData>,
                           @Qualifier("rabbitPublishersThreadPool")
                           private val rabbitPublishersThreadPool: TaskExecutor) {
    @PostConstruct
    private fun init() {
        rabbitPublishersThreadPool.execute {
            Thread.currentThread().name = CashInOutEventSender::class.java.simpleName
            while (true) {
                try {
                    processEventData(cashInOutEventData.take())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@execute
                }
            }
        }
    }

    fun sendEvent(eventData: CashInOutEventData) {
        cashInOutEventData.put(eventData)
    }

    private fun processEventData(eventData: CashInOutEventData) {
        eventSender.forEach {
            it.sendEvent(eventData)
        }
    }
}