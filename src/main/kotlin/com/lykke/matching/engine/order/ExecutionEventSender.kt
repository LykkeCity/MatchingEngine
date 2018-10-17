package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.order.transaction.ExecutionContext
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class ExecutionEventSender(private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                           private val messageSender: MessageSender,
                           private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                           private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                           private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
                           private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<OrderBook>) {

    fun generateSequenceNumbers(executionContext: ExecutionContext): SequenceNumbersWrapper {
        var sequenceNumber: Long? = null
        var clientsSequenceNumber: Long? = null
        var trustedClientsSequenceNumber: Long? = null
        if (isTrustedClientEvent(executionContext.getTrustedClientsLimitOrdersWithTrades())) {
            trustedClientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = trustedClientsSequenceNumber
        }
        if (isClientEvent(executionContext.getClientsLimitOrdersWithTrades(), executionContext.marketOrderWithTrades)) {
            clientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = clientsSequenceNumber
        }
        return SequenceNumbersWrapper(clientsSequenceNumber, trustedClientsSequenceNumber, sequenceNumber)
    }

    fun sendEvents(executionContext: ExecutionContext,
                           sequenceNumbers: SequenceNumbersWrapper) {

        if (executionContext.lkkTrades.isNotEmpty()) {
            lkkTradesQueue.put(executionContext.lkkTrades)
        }

        executionContext.walletOperationsProcessor.sendNotification(executionContext.requestId, executionContext.messageType.name, executionContext.messageId)

        executionContext.orderBooksHolder.tradeInfoList.forEach {
            genericLimitOrderService.putTradeInfo(it)
        }

        executionContext.orderBooksHolder.outgoingOrderBooks.forEach {
            orderBookQueue.put(it)
            rabbitOrderBookQueue.put(it)
        }

        val trustedClientsLimitOrdersWithTrades = executionContext.getTrustedClientsLimitOrdersWithTrades().toMutableList()
        if (isTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(executionContext.messageId, trustedClientsLimitOrdersWithTrades))
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(sequenceNumbers.trustedClientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    trustedClientsLimitOrdersWithTrades))
        }

        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades().toList()
        if (isClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades)) {
            if (clientsLimitOrdersWithTrades.isNotEmpty()) {
                clientLimitOrdersQueue.put(LimitOrdersReport(executionContext.messageId, clientsLimitOrdersWithTrades.toMutableList()))
            }
            executionContext.marketOrderWithTrades?.let { rabbitSwapQueue.put(it) }
            messageSender.sendMessage(EventFactory.createExecutionEvent(sequenceNumbers.clientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    executionContext.walletOperationsProcessor.getClientBalanceUpdates(),
                    clientsLimitOrdersWithTrades,
                    executionContext.marketOrderWithTrades))
        }
    }

    private fun isTrustedClientEvent(trustedClientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>): Boolean {
        return trustedClientsLimitOrdersWithTrades.isNotEmpty()
    }

    private fun isClientEvent(clientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
                              marketOrderWithTrades: MarketOrderWithTrades?): Boolean {
        return clientsLimitOrdersWithTrades.isNotEmpty() || marketOrderWithTrades != null
    }
}