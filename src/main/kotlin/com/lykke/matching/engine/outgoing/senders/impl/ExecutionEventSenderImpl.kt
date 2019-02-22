package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedExecutionEventSender
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.utils.event.isThereClientEvent
import com.lykke.matching.engine.utils.event.isThereTrustedClientEvent
import org.springframework.stereotype.Component

@Component
class ExecutionEventSenderImpl(private val messageSender: MessageSender) : SpecializedExecutionEventSender {

    override fun sendEvent(executionData: ExecutionData) {
        val executionContext = executionData.executionContext
        sendTrustedClientsExecutionEventIfNeeded(executionContext, executionData)
        sendClientsExecutionEventIfNeeded(executionContext, executionData)
    }

    fun sendClientsExecutionEventIfNeeded(executionContext: ExecutionContext, executionEventData: ExecutionData) {
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

    fun sendTrustedClientsExecutionEventIfNeeded(executionContext: ExecutionContext, executionEventData: ExecutionData) {
        val trustedClientsLimitOrdersWithTrades = executionContext.getTrustedClientsLimitOrdersWithTrades().toMutableList()
        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(executionEventData.sequenceNumbers.trustedClientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    trustedClientsLimitOrdersWithTrades))
        }
    }
}