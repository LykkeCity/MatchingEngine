package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.Date
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MultiLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                             private val limitOrderReportQueue: BlockingQueue<JsonSerializable>,
                             private val trustedLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                             private val orderBookQueue: BlockingQueue<OrderBook>,
                             private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                             private val assetsPairsHolder: AssetsPairsHolder,
                             private val negativeSpreadAssets: Set<String>): AbstractService<ProtocolMessages.OldMultiLimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0

    private var logCount = 1000

    private var totalPersistTime: Double = 0.0
    private var totalTime: Double = 0.0

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got multi limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}")

        val orders = ArrayList<LimitOrder>(message.ordersList.size)
        val now = Date()
        val limitOrdersReport = LimitOrdersReport()
        val trustedLimitOrdersReport = LimitOrdersReport()

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
            if (order.remainingVolume != order.volume) {
                trustedLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
            } else {
                limitOrdersReport.orders.add(LimitOrderWithTrades(order))
            }
        }

        var buySide = false
        var sellSide = false

        val pair = assetsPairsHolder.getAssetPair(message.assetPairId)
        orders.forEach { order ->
            if ((negativeSpreadAssets.contains(pair.baseAssetId) || negativeSpreadAssets.contains(pair.quotingAssetId)) && orderBook.leadToNegativeSpread(order)) {
                LOGGER.info("Order ${order.assetPairId}, ${order.volume}, ${order.price} lead to negative spread, ignoring it")
            } else {
                orderBook.addOrder(order)
                limitOrderService.addOrder(order)
                limitOrdersReport.orders.add(LimitOrderWithTrades(order))
                if (order.isBuySide()) buySide = true else sellSide = true
            }
        }

        limitOrderService.setOrderBook(message.assetPairId, orderBook)
        val startPersistTime = System.nanoTime()
        limitOrderService.cancelLimitOrders(ordersToCancel)
        limitOrderService.addOrders(orders)
        val endPersistTime = System.nanoTime()

        val orderBookCopy = orderBook.copy()
        if (buySide) {
            val newOrderBook = OrderBook(message.assetPairId, true, now, orderBookCopy.getOrderBook(true))
            limitOrderService.putTradeInfo(TradeInfo(message.assetPairId, true, orderBookCopy.getBidPrice(), now))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }
        if (sellSide) {
            val newOrderBook = OrderBook(message.assetPairId, false, now, orderBookCopy.getOrderBook(false))
            limitOrderService.putTradeInfo(TradeInfo(message.assetPairId, false, orderBookCopy.getAskPrice(), now))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())

        val endTime = System.nanoTime()

        messagesCount++
        totalPersistTime += (endPersistTime - startPersistTime).toDouble() / logCount
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            LOGGER.info("Total time: ${convertToString(totalTime)}. " +
                    " Persist time: ${convertToString(totalPersistTime)}, ${RoundingUtils.roundForPrint2(100*totalPersistTime/totalTime)} %")
            totalPersistTime = 0.0
            totalTime = 0.0
        }

        if (limitOrdersReport.orders.isNotEmpty()) {
            limitOrderReportQueue.put(limitOrdersReport)
        }

        if (trustedLimitOrdersReport.orders.isNotEmpty()) {
            trustedLimitOrderReportQueue.put(trustedLimitOrdersReport)
        }
    }

    private fun parse(array: ByteArray): ProtocolMessages.OldMultiLimitOrder {
        return ProtocolMessages.OldMultiLimitOrder.parseFrom(array)
    }

    private fun convertToString(value: Double): String {
        if ((value / 100000).toInt() == 0) {
            //microseconds
            return "${RoundingUtils.roundForPrint(value / 1000)} micros ($value nanos)"
        } else {
            //milliseconds
            return "${RoundingUtils.roundForPrint(value / 1000000)} millis ($value nanos)"
        }
    }
}
