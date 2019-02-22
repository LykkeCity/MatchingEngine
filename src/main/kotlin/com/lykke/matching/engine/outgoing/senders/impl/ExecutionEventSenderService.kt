package com.lykke.matching.engine.outgoing.senders.impl

import com.lykke.matching.engine.daos.ExecutionData
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.senders.SpecializedExecutionEventSender
import com.lykke.matching.engine.services.AssetOrderBook
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Service
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Service
class ExecutionEventSenderService(private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                  private val genericLimitOrderService: GenericLimitOrderService,
                                  private val orderBookQueue: BlockingQueue<OrderBook>,
                                  private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                  private val executionEventDataQueue: BlockingQueue<ExecutionData>,
                                  private val specializedExecutionEventSenders: List<SpecializedExecutionEventSender>,
                                  @Qualifier("rabbitPublishersThreadPool")
                           private val rabbitPublishersThreadPool: TaskExecutor) {

    @PostConstruct
    private fun init() {
        rabbitPublishersThreadPool.execute {
            Thread.currentThread().name = ExecutionEventSenderService::class.java.simpleName
            while (true) {
                try {
                    processEvent(executionEventDataQueue.take())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@execute
                }
            }
        }
    }

    fun sendEvents(executionEventData: ExecutionData) {
        executionEventDataQueue.put(executionEventData)
    }

    private fun processEvent(executionData: ExecutionData) {
        sendNonRabbitEvents(executionData.executionContext)
        sendOrderBooksEvents(executionData.executionContext)
        sendExecutionEvent(executionData)
    }

    private fun sendExecutionEvent(executionData: ExecutionData) {
        for (specializedExecutionEventSender in specializedExecutionEventSenders) {
            specializedExecutionEventSender.sendEvent(executionData)
        }
    }

    private fun sendOrderBooksEvents(executionContext: ExecutionContext) {
        executionContext.orderBooksHolder.outgoingOrderBooks.forEach {
            val orders = Array(it.orders.size) { idx -> it.orders[idx] as LimitOrder }
            val orderBook = OrderBook(it.assetPair, it.isBuySide, it.date, AssetOrderBook.sortOrderBook(it.isBuySide, orders))
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