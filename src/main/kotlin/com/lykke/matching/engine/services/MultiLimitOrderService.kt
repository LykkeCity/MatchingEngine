package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
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
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.ArrayList
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MultiLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                             private val trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                             private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                             private val orderBookQueue: BlockingQueue<OrderBook>,
                             private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                             assetsHolder: AssetsHolder,
                             private val assetsPairsHolder: AssetsPairsHolder,
                             private val balancesHolder: BalancesHolder,
                             private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>): AbstractService {

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
        val orders: List<NewLimitOrder>
        val now = Date()
        val trustedClientLimitOrdersReport = LimitOrdersReport()
        val clientLimitOrdersReport = LimitOrdersReport()
        var cancelBuySide = false
        var cancelSellSide = false
        val cancelAllPreviousLimitOrders: Boolean
        val messageUid: String
        val clientId: String
        val assetPairId: String
        val isOldTypeMessage = messageWrapper.type == MessageType.OLD_MULTI_LIMIT_ORDER.type

        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        if (isOldTypeMessage) {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldMultiLimitOrder
            messageUid = message.uid.toString()
            clientId = message.clientId
            assetPairId = message.assetPairId
            LOGGER.debug("Got old multi limit order id: $messageUid, client $clientId, assetPair: $assetPairId")
            orders = ArrayList(message.ordersList.size)
            cancelAllPreviousLimitOrders = message.cancelAllPreviousLimitOrders
            message.ordersList.forEach { currentOrder ->
                val uid = UUID.randomUUID().toString()
                orders.add(NewLimitOrder(uid, uid, message.assetPairId, message.clientId, currentOrder.volume,
                        currentOrder.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, currentOrder.volume, null))

                if (cancelAllPreviousLimitOrders) {
                    if (currentOrder.volume > 0) {
                        cancelBuySide = true
                    } else {
                        cancelSellSide = true
                    }
                }
            }
        } else {
            val message = messageWrapper.parsedMessage!! as ProtocolMessages.MultiLimitOrder
            messageUid = message.uid
            clientId = message.clientId
            assetPairId = message.assetPairId
            LOGGER.debug("Got multi limit order id: $messageUid, client $clientId, assetPair: $assetPairId")
            orders = ArrayList(message.ordersList.size)
            cancelAllPreviousLimitOrders = message.cancelAllPreviousLimitOrders
            message.ordersList.forEach { currentOrder ->
                val uid = UUID.randomUUID().toString()
                orders.add(NewLimitOrder(uid, currentOrder.uid, message.assetPairId, message.clientId, currentOrder.volume,
                        currentOrder.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, currentOrder.volume, null,
                        fee = if (currentOrder.hasFee()) LimitOrderFeeInstruction.create(currentOrder.fee) else null,
                        fees = NewLimitOrderFeeInstruction.create(currentOrder.feesList)))

                if (cancelAllPreviousLimitOrders) {
                    if (currentOrder.volume > 0) {
                        cancelBuySide = true
                    } else {
                        cancelSellSide = true
                    }
                }
            }
        }

        val responseBuilder = ProtocolMessages.MultiLimitOrderResponse.newBuilder()
        responseBuilder.setId(messageUid).setStatus(MessageStatus.OK.type).assetPairId = assetPairId

        val ordersToCancel = ArrayList<NewLimitOrder>()

        if (cancelAllPreviousLimitOrders) {
            if (cancelBuySide) {
                ordersToCancel.addAll(limitOrderService.getAllPreviousOrders(clientId, assetPairId, true))
            }
            if (cancelSellSide) {
                ordersToCancel.addAll(limitOrderService.getAllPreviousOrders(clientId, assetPairId, false))
            }
        }

        val orderBook = limitOrderService.getOrderBook(assetPairId).copy()

        ordersToCancel.forEach { order ->
            orderBook.removeOrder(order)
            if (order.isPartiallyMatched()) {
                clientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
            } else {
                trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
            }
        }

        var buySide = false
        var sellSide = false

        val trades = LinkedList<LkkTrade>()
        val walletOperations = LinkedList<WalletOperation>()
        val ordersToAdd = LinkedList<NewLimitOrder>()
        val assetPair = assetsPairsHolder.getAssetPair(assetPairId)
        val balances = mutableMapOf(
                Pair(assetPair.baseAssetId, balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId)),
                Pair(assetPair.quotingAssetId, balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId)))

        orders.forEach { order ->
            val orderStatusBuilder = ProtocolMessages.MultiLimitOrderResponse.OrderStatus.newBuilder().setId(order.externalId)
                    .setMatchingEngineId(order.id).setStatus(MessageStatus.OK.type).setVolume(order.volume).setPrice(order.price)

            if (order.price <= 0) {
                orderStatusBuilder.status = MessageStatus.INVALID_PRICE.type
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to invalid price")
            } else if (!order.checkVolume(assetPair)) {
                orderStatusBuilder.status = MessageStatus.TOO_SMALL_VOLUME.type
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due too small volume")
            } else if (orderBook.leadToNegativeSpreadForClient(order)) {
                orderStatusBuilder.status = MessageStatus.LEAD_TO_NEGATIVE_SPREAD.type
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to negative spread")
            } else if (orderBook.leadToNegativeSpreadByOtherClient(order)) {
                val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()), balances[if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId])
                val limitOrder = matchingResult.order as NewLimitOrder
                when (OrderStatus.valueOf(matchingResult.order.status)) {
                    OrderStatus.NoLiquidity -> {
                        orderStatusBuilder.status = MessageStatus.NO_LIQUIDITY.type
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    }
                    OrderStatus.NotEnoughFunds -> {
                        orderStatusBuilder.status = MessageStatus.NOT_ENOUGH_FUNDS.type
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    }
                    OrderStatus.InvalidFee -> {
                        orderStatusBuilder.status = MessageStatus.INVALID_FEE.type
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(limitOrder))
                    }
                    OrderStatus.Matched,
                    OrderStatus.Processing-> {
                        limitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders)

                        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                            val result = limitOrderService.cancelNotEnoughFundsOrder(NotEnoughFundsLimitOrderCancelParams(matchingResult.cancelledLimitOrders.toList()))
                            walletOperations.addAll(result.walletOperation)
                            trustedClientLimitOrdersReport.orders.addAll(result.trustedClientLimitOrderWithTrades)
                            clientLimitOrdersReport.orders.addAll(result.clientLimitOrderWithTrades)
                        }
                        cancelBuySide = cancelBuySide || matchingResult.cancelledLimitOrders.any { it.isBuySide() }
                        cancelSellSide = cancelSellSide || matchingResult.cancelledLimitOrders.any { !it.isBuySide() }

                        matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                        if (matchingResult.uncompletedLimitOrder != null) {
                            matchingResult.orderBook.put(matchingResult.uncompletedLimitOrder)
                        }

                        orderBook.setOrderBook(!order.isBuySide(), matchingResult.orderBook)

                        trades.addAll(matchingResult.lkkTrades)
                        var limitOrderWithTrades = clientLimitOrdersReport.orders.find { it.order.externalId == limitOrder.externalId}
                        if (limitOrderWithTrades == null) {
                            limitOrderWithTrades = LimitOrderWithTrades(limitOrder)
                            clientLimitOrdersReport.orders.add(limitOrderWithTrades)
                        }

                        limitOrderWithTrades.trades.addAll(matchingResult.marketOrderTrades.map { it ->
                            LimitTradeInfo(it.tradeId, it.marketClientId, it.marketAsset, it.marketVolume, it.price, matchingResult.timestamp, it.limitOrderId, it.limitOrderExternalId, it.limitAsset, it.limitClientId, it.limitVolume, it.feeInstruction, it.feeTransfer, it.fees)
                        })

                        matchingResult.limitOrdersReport?.orders?.forEach { orderReport ->
                            var trustedOrder = clientLimitOrdersReport.orders.find { it.order.externalId == orderReport.order.externalId}
                            if (trustedOrder == null) {
                                trustedOrder = LimitOrderWithTrades(orderReport.order)
                                clientLimitOrdersReport.orders.add(trustedOrder)
                            }
                            trustedOrder.trades.addAll(orderReport.trades)
                        }

                        walletOperations.addAll(matchingResult.cashMovements)
                        if (matchingResult.order.status == OrderStatus.Processing.name) {
                            ordersToAdd.add(order)
                            orderBook.addOrder(order)
                        }
                        balances[if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId] = matchingResult.marketBalance!!

                        sellSide = true
                        buySide = true
                    }
                    else -> {
                    }
                }
            } else {
                ordersToAdd.add(order)
                orderBook.addOrder(order)
                trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                if (order.isBuySide()) buySide = true else sellSide = true
            }

            responseBuilder.addStatuses(orderStatusBuilder.build())
        }

        val startPersistTime = System.nanoTime()
        balancesHolder.processWalletOperations(messageUid, MessageType.MULTI_LIMIT_ORDER.name, walletOperations)
        lkkTradesQueue.put(trades)
        limitOrderService.cancelLimitOrders(ordersToCancel)
        limitOrderService.addOrders(ordersToAdd)
        limitOrderService.setOrderBook(assetPairId, orderBook)
        if (buySide || cancelBuySide) {
            limitOrderService.updateOrderBook(assetPairId, true)
        }
        if (sellSide || cancelSellSide) {
            limitOrderService.updateOrderBook(assetPairId, false)
        }
        val endPersistTime = System.nanoTime()

        val orderBookCopy = orderBook.copy()
        if (buySide || cancelBuySide) {
            val newOrderBook = OrderBook(assetPairId, true, now, orderBookCopy.getOrderBook(true))
            limitOrderService.putTradeInfo(TradeInfo(assetPairId, true, orderBook.getBidPrice(), now))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }
        if (sellSide || cancelSellSide) {
            val newOrderBook = OrderBook(assetPairId, false, now, orderBookCopy.getOrderBook(false))
            limitOrderService.putTradeInfo(TradeInfo(assetPairId, false, orderBook.getAskPrice(), now))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }

        if (isOldTypeMessage) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(messageUid.toLong()).build())
        } else {
            messageWrapper.writeMultiLimitOrderResponse(responseBuilder.build())
        }

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

        if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
            trustedClientLimitOrderReportQueue.put(trustedClientLimitOrdersReport)
        }

        if (clientLimitOrdersReport.orders.isNotEmpty()) {
            clientLimitOrderReportQueue.put(clientLimitOrdersReport)
        }
    }

    private fun parseOldMultiLimitOrder(array: ByteArray): ProtocolMessages.OldMultiLimitOrder {
        return ProtocolMessages.OldMultiLimitOrder.parseFrom(array)
    }

    private fun parseMultiLimitOrder(array: ByteArray): ProtocolMessages.MultiLimitOrder {
        return ProtocolMessages.MultiLimitOrder.parseFrom(array)
    }

    override fun parseMessage(messageWrapper: MessageWrapper) {
        if (messageWrapper.type == MessageType.OLD_MULTI_LIMIT_ORDER.type) {
            val message =  parseOldMultiLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        } else {
            val message =  parseMultiLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = message.uid
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_MULTI_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder().setUid(messageWrapper.messageId!!.toLong()).build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder().setId(messageWrapper.messageId!!).setStatus(status.type).build())
        }
    }
}
