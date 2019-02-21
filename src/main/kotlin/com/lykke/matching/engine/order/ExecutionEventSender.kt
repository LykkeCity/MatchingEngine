package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.senders.SpecializedExecutionEventSender
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class ExecutionEventSender(private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                           private val specializedExecutionEventSenders: List<SpecializedExecutionEventSender>) {

    fun sendEvents(executionContext: ExecutionContext,
                   sequenceNumbers: SequenceNumbersWrapper) {
        sendNonRabbitEvents(executionContext)
        sendOrderBooksEvents(executionContext)
        sendExecutionEvent(executionContext, sequenceNumbers)
    }

    fun sendExecutionEvent(executionContext: ExecutionContext, sequenceNumbers: SequenceNumbersWrapper) {
        val executionEventData = ExecutionData(executionContext, sequenceNumbers)
        for (specializedExecutionEventSender in specializedExecutionEventSenders) {
            specializedExecutionEventSender.sendEvent(executionEventData)
        }
    }

    fun sendOrderBooksEvents(executionContext: ExecutionContext) {
        executionContext.orderBooksHolder.outgoingOrderBooks.forEach {
            orderBookQueue.put(it)
            rabbitOrderBookQueue.put(it)
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