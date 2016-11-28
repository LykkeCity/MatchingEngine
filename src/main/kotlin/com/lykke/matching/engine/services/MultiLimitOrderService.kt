package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.JsonSerializable
import com.lykke.matching.engine.outgoing.OrderBook
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MultiLimitOrderService(val limitOrderService: GenericLimitOrderService,
                             val orderBookQueue: BlockingQueue<JsonSerializable>,
                             val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>): AbsractService<ProtocolMessages.MultiLimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got multi limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}")

        val orders = ArrayList<LimitOrder>(message.ordersList.size)
        val now = Date()

        var cancelBuySide = false
        var cancelSellSide = false

        message.ordersList.forEach { currentOrder ->
            val uid = UUID.randomUUID().toString()
            orders.add(LimitOrder(uid, uid, message.assetPairId, message.clientId, currentOrder.volume,
                    currentOrder.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, currentOrder.volume, null))

            if (message.cancelAllPreviousLimitOrders) {
                if (currentOrder.volume > 0) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }

        val ordersToCancel = ArrayList<LimitOrder>()

        if (message.cancelAllPreviousLimitOrders) {
            if (cancelBuySide) {
                ordersToCancel.addAll(limitOrderService.getAllPreviousOrders(message.clientId, message.assetPairId, true))
            }
            if (cancelSellSide) {
                ordersToCancel.addAll(limitOrderService.getAllPreviousOrders(message.clientId, message.assetPairId, false))
            }
        }

        val orderBook = limitOrderService.getOrderBook(message.assetPairId).copy()

        ordersToCancel.forEach { order ->
            orderBook.removeOrder(order)
        }

        var buySide = false
        var sellSide = false

        orders.forEach { order ->
            orderBook.addOrder(order)
            limitOrderService.addOrder(order)
            limitOrderService.putTradeInfo(TradeInfo(order.assetPairId, order.isBuySide(), order.price, now))
            if (order.isBuySide()) buySide = true else sellSide = true

            LOGGER.info("Part of multi limit order id: ${message.uid}, client ${order.clientId}, assetPair: ${order.assetPairId}, volume: ${RoundingUtils.roundForPrint(order.volume)}, price: ${RoundingUtils.roundForPrint(order.price)} added to order book")
        }

        limitOrderService.setOrderBook(message.assetPairId, orderBook)
        limitOrderService.cancelLimitOrders(ordersToCancel)
        limitOrderService.addOrders(orders)

        val orderBookCopy = orderBook.copy()
        if (buySide) {
            val newOrderBook = OrderBook(message.assetPairId, true, now, orderBookCopy.getOrderBook(true))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }
        if (sellSide) {
            val newOrderBook = OrderBook(message.assetPairId, false, now, orderBookCopy.getOrderBook(false))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())
    }

    private fun parse(array: ByteArray): ProtocolMessages.MultiLimitOrder {
        return ProtocolMessages.MultiLimitOrder.parseFrom(array)
    }
}
