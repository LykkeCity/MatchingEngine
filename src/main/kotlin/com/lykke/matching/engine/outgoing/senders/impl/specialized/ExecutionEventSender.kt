package com.lykke.matching.engine.outgoing.senders.impl.specialized

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.OutgoingEventData
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.senders.SpecializedEventSender
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.utils.event.isThereClientEvent
import com.lykke.matching.engine.utils.event.isThereTrustedClientEvent
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class ExecutionEventSender(private val messageSender: MessageSender,
                           private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<OrderBook>) : SpecializedEventSender {
    override fun getProcessedMessageClass(): Class<*> {
        return ExecutionData::class.java
    }

    override fun sendEvent(outgointData: OutgoingEventData) {
        val executionData = outgointData.eventData as  ExecutionData

        sendNonRabbitEvents(executionData.executionContext)
        sendOrderBooksEvents(executionData.executionContext)

        sendTrustedClientsExecutionEventIfNeeded(executionData)
        sendClientsExecutionEventIfNeeded(executionData)
    }

    private fun sendClientsExecutionEventIfNeeded(executionData: ExecutionData) {
        val executionContext = executionData.executionContext

        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades().toList()
        if (isThereClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades)) {
            messageSender.sendMessage(EventFactory.createExecutionEvent(executionData.sequenceNumbers.clientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    executionContext.walletOperationsProcessor.getClientBalanceUpdates(),
                    clientsLimitOrdersWithTrades,
                    executionContext.marketOrderWithTrades))
        }
    }

    private fun sendTrustedClientsExecutionEventIfNeeded(executionData: ExecutionData) {
        val executionContext = executionData.executionContext

        val trustedClientsLimitOrdersWithTrades = executionContext.getTrustedClientsLimitOrdersWithTrades().toList()
        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(executionData.sequenceNumbers.trustedClientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    trustedClientsLimitOrdersWithTrades))
        }
    }

    private fun sendOrderBooksEvents(executionContext: ExecutionContext) {
        executionContext.orderBooksHolder.outgoingOrderBooks.forEach {
            val orderBook = OrderBook(it.assetPair, it.isBuySide, it.date, AssetOrderBook.sortOrderBook(it.isBuySide, it.orders))
            orderBookQueue.put(orderBook)
            rabbitOrderBookQueue.put(orderBook)
        }
    }

    private fun sendNonRabbitEvents(executionContext: ExecutionContext) {
        if (executionContext.lkkTrades.isNotEmpty()) {
            lkkTradesQueue.put(executionContext.lkkTrades)
        }

        executionContext.orderBooksHolder.tradeInfoList.forEach {
            genericLimitOrderService.putTradeInfo(it)
        }
    }
}