package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.senders.SpecializedCashTransferEventSender
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class CashTransferEventSender(
        private val cashTransferEventData: BlockingQueue<CashTransferEventData>,
        private val eventSenders: List<SpecializedCashTransferEventSender>,
        @Qualifier("rabbitPublishersThreadPool")
        private val rabbitPublishersThreadPool: TaskExecutor) {

    @PostConstruct
    private fun init() {
        rabbitPublishersThreadPool.execute {
            Thread.currentThread().name = CashTransferEventSender::class.java.simpleName
            while (true) {
                try {
                    processEvent(cashTransferEventData.take())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@execute
                }
            }
        }
    }

    fun sendEvent(eventData: CashTransferEventData) {
        cashTransferEventData.put(eventData)
    }

    private fun processEvent(cashTransferEventData: CashTransferEventData) {
        for (eventSender in eventSenders) {
            eventSender.sendEvent(cashTransferEventData)
        }
    }
}