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
    override fun getEventClass(): Class<*> {
        return ExecutionData::class.java
    }

    override fun sendEvent(eventData: OutgoingEventData) {
        val executionData = eventData.eventData as  ExecutionData

        sendNonRabbitEvents(executionData.executionContext)
        sendOrderBooksEvents(executionData.executionContext)

        sendTrustedClientsExecutionEventIfNeeded(executionData)
        sendClientsExecutionEventIfNeeded(executionData)
    }

    private fun sendClientsExecutionEventIfNeeded(executionData: ExecutionData) {
        val executionContext = executionData.executionContext

        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades().toList()
        if (isThereClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades)) {
            messageSender.sendMessage(EventFactory.createExecutionEvent(sequenceNumber = executionData.sequenceNumbers.clientsSequenceNumber!!,
                    messageId =  executionContext.messageId,
                    requestId = executionContext.requestId,
                    date = executionContext.date,
                    messageType = executionContext.messageType,
                    clientBalanceUpdates =  executionContext.walletOperationsProcessor.getClientBalanceUpdates(),
                    limitOrdersWithTrades =  clientsLimitOrdersWithTrades,
                    marketOrderWithTrades =  executionContext.marketOrderWithTrades))
        }
    }

    private fun sendTrustedClientsExecutionEventIfNeeded(executionData: ExecutionData) {
        val executionContext = executionData.executionContext

        val trustedClientsLimitOrdersWithTrades = executionContext.getTrustedClientsLimitOrdersWithTrades().toList()
        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(sequenceNumber =  executionData.sequenceNumbers.trustedClientsSequenceNumber!!,
                    messageId = executionContext.messageId,
                    requestId = executionContext.requestId,
                    date = executionContext.date,
                    messageType = executionContext.messageType,
                    limitOrdersWithTrades =  trustedClientsLimitOrdersWithTrades))
        }
    }

    private fun sendOrderBooksEvents(executionContext: ExecutionContext) {
        executionContext.orderBooksHolder.outgoingOrderBooks.forEach {
            val orderBook = OrderBook(it.assetPair, it.isBuySide, it.date, AssetOrderBook.sort(it.isBuySide, it.volumePrices))
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