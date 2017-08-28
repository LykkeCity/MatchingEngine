package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MultiLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                             private val limitOrderReportQueue: BlockingQueue<JsonSerializable>,
                             private val trustedLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                             private val orderBookQueue: BlockingQueue<OrderBook>,
                             private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                             private val assetsHolder: AssetsHolder,
                             private val assetsPairsHolder: AssetsPairsHolder,
                             private val negativeSpreadAssets: Set<String>,
                             private val balancesHolder: BalancesHolder,
                             private val marketOrderDatabaseAccessor: MarketOrderDatabaseAccessor): AbstractService<ProtocolMessages.OldMultiLimitOrder> {

    companion object {
        val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
        val STATS_LOGGER = Logger.getLogger("${MultiLimitOrderService::class.java.name}.stats")
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var messagesCount: Long = 0
    private var ordersCount: Long = 0
    private var logCount = 1000
    private var totalPersistTime: Double = 0.0
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
        val message = parse(messageWrapper.byteArray)
        LOGGER.debug("Got multi limit order id: ${message.uid}, client ${message.clientId}, assetPair: ${message.assetPairId}")

        val orders = ArrayList<NewLimitOrder>(message.ordersList.size)
        val now = Date()
        val limitOrdersReport = LimitOrdersReport()
        val trustedLimitOrdersReport = LimitOrdersReport()

        var cancelBuySide = false
        var cancelSellSide = false

        message.ordersList.forEach { currentOrder ->
            val uid = UUID.randomUUID().toString()
            orders.add(NewLimitOrder(uid, uid, message.assetPairId, message.clientId, currentOrder.volume,
                    currentOrder.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, currentOrder.volume, null))

            if (message.cancelAllPreviousLimitOrders) {
                if (currentOrder.volume > 0) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }

        val ordersToCancel = ArrayList<NewLimitOrder>()

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

        val trades = LinkedList<LkkTrade>()
        val walletOperations = LinkedList<WalletOperation>()
        val ordersToAdd = LinkedList<NewLimitOrder>()
        orders.forEach { order ->
            if (orderBook.leadToNegativeSpreadByOtherClient(order)) {
                val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()))
                val limitOrder = matchingResult.order as NewLimitOrder
                when (OrderStatus.valueOf(matchingResult.order.status)) {
                    OrderStatus.NoLiquidity -> {
                        limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    }
                    OrderStatus.NotEnoughFunds -> {
                        limitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    }
                    OrderStatus.Matched,
                    OrderStatus.Processing-> {
                        limitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders)
                        matchingResult.cancelledLimitOrders.forEach { it ->
                            it.status = OrderStatus.NotEnoughFunds.name
                        }
                        limitOrderService.moveOrdersToDone(ArrayList<NewLimitOrder>(matchingResult.cancelledLimitOrders))

                        matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                        if (matchingResult.uncompletedLimitOrder != null) {
                            matchingResult.orderBook.put(matchingResult.uncompletedLimitOrder)
                        }

                        limitOrderService.setOrderBook(order.assetPairId, !order.isBuySide(), matchingResult.orderBook)

                        trades.addAll(matchingResult.lkkTrades)
                        var limitOrderWithTrades = trustedLimitOrdersReport.orders.find { it.order.externalId == limitOrder.externalId}
                        if (limitOrderWithTrades == null) {
                            limitOrderWithTrades = LimitOrderWithTrades(limitOrder)
                            trustedLimitOrdersReport.orders.add(limitOrderWithTrades)
                        }

                        limitOrderWithTrades.trades.addAll(matchingResult.marketOrderTrades.map { it ->
                            LimitTradeInfo(it.marketClientId, it.marketAsset, it.marketVolume, it.price, now, it.limitOrderId, it.limitOrderExternalId, it.limitAsset, it.limitClientId, it.limitVolume)
                        })

                        matchingResult.limitOrdersReport?.orders?.forEach { order ->
                            var trustedOrder = trustedLimitOrdersReport.orders.find { it.order.externalId == order.order.externalId}
                            if (trustedOrder == null) {
                                trustedOrder = LimitOrderWithTrades(order.order)
                                trustedLimitOrdersReport.orders.add(trustedOrder)
                            }
                            trustedOrder.trades.addAll(order.trades)
                        }

                        walletOperations.addAll(matchingResult.cashMovements)
                        if (matchingResult.order.status == OrderStatus.Processing.name) {
                            ordersToAdd.add(order)
                            orderBook.addOrder(order)
                        }

                        sellSide = true
                        buySide = true
                    }
                    else -> {
                    }
                }
            } else {
                ordersToAdd.add(order)
                orderBook.addOrder(order)
                limitOrdersReport.orders.add(LimitOrderWithTrades(order))
                if (order.isBuySide()) buySide = true else sellSide = true
            }
        }

        val startPersistTime = System.nanoTime()
        balancesHolder.processWalletOperations(message.uid.toString(), MessageType.MULTI_LIMIT_ORDER.name, walletOperations)
        marketOrderDatabaseAccessor.addLkkTrades(trades)
        limitOrderService.addOrders(ordersToAdd)
        limitOrderService.cancelLimitOrders(ordersToCancel)
        limitOrderService.setOrderBook(message.assetPairId, orderBook)
        if (buySide || cancelBuySide) {
            limitOrderService.updateOrderBook(message.assetPairId, true)
        }
        if (sellSide || cancelSellSide) {
            limitOrderService.updateOrderBook(message.assetPairId, false)
        }
        val endPersistTime = System.nanoTime()

        val orderBookCopy = orderBook.copy()
        if (buySide || cancelBuySide) {
            val newOrderBook = OrderBook(message.assetPairId, true, now, orderBookCopy.getOrderBook(true))
            limitOrderService.putTradeInfo(TradeInfo(message.assetPairId, true, orderBookCopy.getBidPrice(), now))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }
        if (sellSide || cancelSellSide) {
            val newOrderBook = OrderBook(message.assetPairId, false, now, orderBookCopy.getOrderBook(false))
            limitOrderService.putTradeInfo(TradeInfo(message.assetPairId, false, orderBookCopy.getAskPrice(), now))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(message.uid).build())

        val endTime = System.nanoTime()

        messagesCount++
        ordersCount += orders.size
        totalPersistTime += (endPersistTime - startPersistTime).toDouble() / logCount
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Orders: $ordersCount/$logCount messages. Total: ${PrintUtils.convertToString(totalTime)}. " +
                    " Persist: ${PrintUtils.convertToString(totalPersistTime)}, ${RoundingUtils.roundForPrint2(100*totalPersistTime/totalTime)} %")
            ordersCount = 0
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
}
