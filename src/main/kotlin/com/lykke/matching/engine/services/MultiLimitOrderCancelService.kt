package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.Date
import java.util.concurrent.BlockingQueue

class MultiLimitOrderCancelService(private val limitOrderService: GenericLimitOrderService,
                                   private val orderBookQueue: BlockingQueue<OrderBook>,
                                   private val limitOrderReportQueue: BlockingQueue<JsonSerializable>,
                                   private val trustedLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                                   private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>): AbstractService<ProtocolMessages.MultiLimitOrderCancel> {

    companion object {
        val LOGGER = Logger.getLogger(MultiLimitOrderCancelService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got multi limit order cancel id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, isBuy: ${message.isBuy}")

        val now = Date()

        val ordersToCancel = limitOrderService.getAllPreviousOrders(message.clientId, message.assetPairId, message.isBuy)

        if (ordersToCancel.isNotEmpty()) {
            val limitOrdersReport = LimitOrdersReport()
            val trustedLimitOrdersReport = LimitOrdersReport()

            val orderBook = limitOrderService.getOrderBook(message.assetPairId).copy()

            ordersToCancel.forEach { order ->
                orderBook.removeOrder(order)
                if (order.isPartiallyMatched()) {
                    trustedLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                } else {
                    limitOrdersReport.orders.add(LimitOrderWithTrades(order))
                }
            }

            limitOrderService.setOrderBook(message.assetPairId, orderBook)
            limitOrderService.cancelLimitOrders(ordersToCancel)
            limitOrderService.updateOrderBook(message.assetPairId, message.isBuy)

            val orderBookCopy = orderBook.copy()

            val newOrderBook = OrderBook(message.assetPairId, message.isBuy, now, orderBookCopy.getOrderBook(message.isBuy))
            limitOrderService.putTradeInfo(TradeInfo(message.assetPairId,  message.isBuy, if (message.isBuy) orderBook.getBidPrice() else orderBook.getAskPrice(), now))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)

            if (limitOrdersReport.orders.isNotEmpty()) {
                limitOrderReportQueue.put(limitOrdersReport)
            }

            if (trustedLimitOrdersReport.orders.isNotEmpty()) {
                trustedLimitOrderReportQueue.put(trustedLimitOrdersReport)
            }
        }
        messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(message.uid).setStatus(MessageStatus.OK.type).build())
        LOGGER.debug("Multi limit order cancel id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}, isBuy: ${message.isBuy} processed")
    }

    private fun parse(array: ByteArray): ProtocolMessages.MultiLimitOrderCancel {
        return ProtocolMessages.MultiLimitOrderCancel.parseFrom(array)
    }
}
