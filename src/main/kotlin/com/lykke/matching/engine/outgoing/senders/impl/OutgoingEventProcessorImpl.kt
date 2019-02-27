package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.messages.OutgoingEventData
import com.lykke.matching.engine.outgoing.senders.OutgoingEventProcessor
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class OutgoingEventProcessorImpl(private val outgoingEvents: BlockingQueue<OutgoingEventData>,
                                 private val messageSendersByEventClass: Map<Class<*>, List<SpecializedEventSender>>,
                                 @Qualifier("rabbitPublishersThreadPool")
                                 private val rabbitPublishersThreadPool: TaskExecutor): OutgoingEventProcessor {
    @PostConstruct
    private fun init() {
        rabbitPublishersThreadPool.execute {
            Thread.currentThread().name = OutgoingEventProcessorImpl::class.java.simpleName
            while (true) {
                try {
                    processEvent(outgoingEvents.take())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@execute
                }
            }
        }
    }

    override fun submitCashTransferEvent(cashTransferEventData: CashTransferEventData) {
        submitEvent(OutgoingEventData(CashTransferEventData::class.java, cashTransferEventData))
    }

    override fun submitCashInOutEvent(cashInOutEventData: CashInOutEventData) {
        submitEvent(OutgoingEventData(CashInOutEventData::class.java, cashInOutEventData))
    }

    override fun submitExecutionEvent(executionEventData: ExecutionData) {
        submitEvent(OutgoingEventData(ExecutionData::class.java, executionEventData))
    }

    private fun submitEvent(outgoingEventData: OutgoingEventData) {
        outgoingEvents.put(outgoingEventData)
    }

    private fun processEvent(eventData: OutgoingEventData) {
        messageSendersByEventClass[eventData.eventClass]?.forEach {
            it.sendEvent(eventData)
        }
    }
}