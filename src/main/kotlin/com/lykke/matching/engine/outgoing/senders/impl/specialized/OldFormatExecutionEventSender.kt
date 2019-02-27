package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OutgoingEventData
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.utils.event.isThereClientEvent
import com.lykke.matching.engine.utils.event.isThereTrustedClientEvent
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Deprecated("Old format of outgoing message is deprecated")
@Component
class OldFormatExecutionEventSender(private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                    private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                    private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>) : SpecializedEventSender {

    override fun getEventClass(): Class<*> {
        return ExecutionData::class.java
    }

    override fun sendEvent(eventData: OutgoingEventData) {
        val executionData = eventData.eventData as ExecutionData
        val executionContext = executionData.executionContext
        sendBalanceUpdateEvent(executionContext)
        sendTrustedClientsExecutionEventIfNeeded(executionContext)
        sendClientsExecutionEventIfNeeded(executionContext)
    }

    private fun sendBalanceUpdateEvent(executionContext: ExecutionContext) {
        executionContext
                .walletOperationsProcessor
                .sendNotification(id = executionContext.requestId,
                        type = executionContext.messageType.name,
                        messageId = executionContext.messageId)
    }

    private fun sendTrustedClientsExecutionEventIfNeeded(executionContext: ExecutionContext) {
        val trustedClientsLimitOrdersWithTrades = executionContext.getTrustedClientsLimitOrdersWithTrades()

        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(executionContext.messageId, trustedClientsLimitOrdersWithTrades.toMutableList()))
        }
    }

    private fun sendClientsExecutionEventIfNeeded(executionContext: ExecutionContext) {
        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades()
        if (isThereClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades)) {
            if (clientsLimitOrdersWithTrades.isNotEmpty()) {
                clientLimitOrdersQueue.put(LimitOrdersReport(executionContext.messageId, clientsLimitOrdersWithTrades.toMutableList()))
            }
            executionContext.marketOrderWithTrades?.let { rabbitSwapQueue.put(it) }
        }
    }
}