package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.outgoing.messages.CashInOutEventData
import com.lykke.matching.engine.outgoing.messages.CashTransferEventData
import com.lykke.matching.engine.outgoing.messages.OutgoingEventDataWrapper
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
class OutgoingEventProcessorImpl(private val outgoingEventDataWrapperQueue: BlockingQueue<OutgoingEventDataWrapper<*>>,
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
                    processEvent(outgoingEventDataWrapperQueue.take())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@execute
                }
            }
        }
    }

    override fun submitCashTransferEvent(cashTransferEventData: CashTransferEventData) {
        submitEvent(OutgoingEventDataWrapper(CashTransferEventData::class.java, cashTransferEventData))
    }

    override fun submitCashInOutEvent(cashInOutEventData: CashInOutEventData) {
        submitEvent(OutgoingEventDataWrapper(CashInOutEventData::class.java, cashInOutEventData))
    }

    override fun submitExecutionEvent(executionEventData: ExecutionData) {
        submitEvent(OutgoingEventDataWrapper(ExecutionData::class.java, executionEventData))
    }

    private fun submitEvent(outgoingEventData: OutgoingEventDataWrapper<*>) {
        outgoingEventDataWrapperQueue.put(outgoingEventData)
    }

    private fun processEvent(outgoingEventDataWrapper: OutgoingEventDataWrapper<*>) {
        val eventSenders = specializedEventSendersByHandledClass[outgoingEventDataWrapper.eventClass]
        if (CollectionUtils.isEmpty(eventSenders)) {
            LOGGER.warn("Sender for class: ${outgoingEventDataWrapper.eventClass}, was not found, event is ignored")
        }
        eventSenders?.forEach {
            it.sendEvent(outgoingEventDataWrapper.eventData!!)
        }
    }
}