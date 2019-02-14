package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.utils.event.isThereClientEvent
import com.lykke.matching.engine.utils.event.isThereTrustedClientEvent
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class ExecutionEventSender(private val messageSender: MessageSender,
                           private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                           private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                           private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
                           private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitEventsDataQueue: BlockingQueue<ExecutionEventSender.RabbitEventData>,
                           @Qualifier("rabbitPublishersThreadPool")
                           private val rabbitPublishersThreadPool: TaskExecutor) {

    @PostConstruct
    private fun init() {
        rabbitPublishersThreadPool.execute {
            Thread.currentThread().name = ExecutionEventSender::class.java.simpleName
            while(true) {
                sendEvents(rabbitEventsDataQueue.take())
            }
        }
    }

    fun sendEvents(executionContext: ExecutionContext,
                   sequenceNumbers: SequenceNumbersWrapper) {
        rabbitEventsDataQueue.put(RabbitEventData(executionContext, sequenceNumbers))
    }

    private fun sendEvents(event: ExecutionEventSender.RabbitEventData) {
        val executionContext = event.executionContext
        val sequenceNumbers = event.sequenceNumbers
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
        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(executionContext.messageId, trustedClientsLimitOrdersWithTrades))
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(sequenceNumbers.trustedClientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    trustedClientsLimitOrdersWithTrades))
        }

        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades().toList()
        if (isThereClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades)) {
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

    class RabbitEventData(val executionContext: ExecutionContext,
                          val sequenceNumbers: SequenceNumbersWrapper)
}