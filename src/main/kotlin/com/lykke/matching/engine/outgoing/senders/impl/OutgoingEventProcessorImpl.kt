package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.daos.OutgoingEventData
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.messages.ReservedCashInOutEventData
import com.lykke.matching.engine.outgoing.senders.OutgoingEventProcessor
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Component
import org.springframework.util.CollectionUtils
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class OutgoingEventProcessorImpl(private val outgoingEventDataQueue: BlockingQueue<OutgoingEventData>,
                                 private val specializedEventSendersByHandledClass: Map<Class<*>, List<SpecializedEventSender<*>>>,
                                 @Qualifier("rabbitPublishersThreadPool")
                                 private val rabbitPublishersThreadPool: TaskExecutor): OutgoingEventProcessor {

    private companion object {
        val LOGGER = ThrottlingLogger.getLogger(OutgoingEventProcessorImpl::class.java.name)
    }

    @PostConstruct
    private fun init() {
        rabbitPublishersThreadPool.execute {
            Thread.currentThread().name = OutgoingEventProcessorImpl::class.java.simpleName
            while (true) {
                try {
                    processEvent(outgoingEventDataQueue.take())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@execute
                }
            }
        }
    }

    override fun submitCashTransferEvent(cashTransferEventData: CashTransferEventData) {
        submitEvent(cashTransferEventData)
    }

    override fun submitCashInOutEvent(cashInOutEventData: CashInOutEventData) {
        submitEvent(cashInOutEventData)
    }

    override fun submitExecutionEvent(executionEventData: ExecutionData) {
        submitEvent(executionEventData)
    }

    override fun submitReservedCashInOutEvent(reservedCashInOutEventData: ReservedCashInOutEventData) {
        submitEvent(reservedCashInOutEventData)
    }

    private fun submitEvent(outgoingEventData: OutgoingEventData) {
        outgoingEventDataQueue.put(outgoingEventData)
    }

    private fun processEvent(eventData: OutgoingEventData) {
        val eventSenders = specializedEventSendersByHandledClass[eventData::class.java]
        if (CollectionUtils.isEmpty(eventSenders)) {
            LOGGER.warn("Sender for class: ${eventData::class.java.name}, was not found, event is ignored")
        }
        eventSenders?.forEach {
            it.sendEvent(eventData)
        }
    }
}