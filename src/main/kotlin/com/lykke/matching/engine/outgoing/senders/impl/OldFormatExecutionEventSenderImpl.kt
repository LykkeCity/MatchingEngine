package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.senders.SpecializedExecutionEventSender
import com.lykke.matching.engine.utils.event.isThereClientEvent
import com.lykke.matching.engine.utils.event.isThereTrustedClientEvent
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Deprecated("Old format of outgoing message is deprecated")
@Component
class OldFormatExecutionEventSenderImpl(private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                        private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                        private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>) : SpecializedExecutionEventSender {


    override fun sendEvent(executionData: ExecutionData) {
        val executionContext = executionData.executionContext
        sendBalanceUpdateEvent(executionContext)
        sendTrustedClientsExecutionEventIfNeeded(executionContext)
        sendClientsExecutionEventIfNeeded(executionContext)
    }

    fun sendBalanceUpdateEvent(executionContext: ExecutionContext) {
        executionContext.walletOperationsProcessor.sendNotification(executionContext.requestId, executionContext.messageType.name, executionContext.messageId)
    }

    fun sendTrustedClientsExecutionEventIfNeeded(executionContext: ExecutionContext) {
        val trustedClientsLimitOrdersWithTrades = executionContext.getTrustedClientsLimitOrdersWithTrades().toMutableList()

        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(executionContext.messageId, trustedClientsLimitOrdersWithTrades.toMutableList()))
        }
    }

    fun sendClientsExecutionEventIfNeeded(executionContext: ExecutionContext) {
        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades().toList()
        if (isThereClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades)) {
            if (clientsLimitOrdersWithTrades.isNotEmpty()) {
                clientLimitOrdersQueue.put(LimitOrdersReport(executionContext.messageId, clientsLimitOrdersWithTrades.toMutableList()))
            }
            executionContext.marketOrderWithTrades?.let { rabbitSwapQueue.put(it) }
        }
    }
}