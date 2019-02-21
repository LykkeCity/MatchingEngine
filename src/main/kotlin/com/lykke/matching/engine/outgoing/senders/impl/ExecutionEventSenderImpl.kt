package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedExecutionEventSender
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.utils.event.isThereClientEvent
import com.lykke.matching.engine.utils.event.isThereTrustedClientEvent
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class ExecutionEventSenderImpl(private val messageSender: MessageSender,
                               private val executionEventDataQueue: BlockingQueue<ExecutionData>,
                               @Qualifier("rabbitPublishersThreadPool")
                               private val rabbitPublishersThreadPool: TaskExecutor) : SpecializedExecutionEventSender {

    @PostConstruct
    private fun init() {
        rabbitPublishersThreadPool.execute {
            Thread.currentThread().name = ExecutionEventSenderImpl::class.java.simpleName
            while (true) {
                try {
                    processEventData(executionEventDataQueue.take())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@execute
                }
            }
        }
    }

    override fun sendEvent(executionEventData: ExecutionData) {
        executionEventDataQueue.put(executionEventData)
    }

    fun processEventData(executionEventData: ExecutionData) {
        val executionContext = executionEventData.executionContext
        val trustedClientsLimitOrdersWithTrades = executionContext.getTrustedClientsLimitOrdersWithTrades().toMutableList()

        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(executionEventData.sequenceNumbers.trustedClientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    trustedClientsLimitOrdersWithTrades))
        }

        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades().toList()

        if (isThereClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades)) {
            messageSender.sendMessage(EventFactory.createExecutionEvent(executionEventData.sequenceNumbers.clientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    executionContext.walletOperationsProcessor.getClientBalanceUpdates(),
                    clientsLimitOrdersWithTrades,
                    executionContext.marketOrderWithTrades))
        }
    }
}