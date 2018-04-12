package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.fee.listOfLimitOrderFee
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
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.RoundingUtils
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
        private val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${MultiLimitOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var ordersCount: Long = 0
    private var logCount = 1000
    private var totalPersistTime: Double = 0.0
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder)
    private val orderServiceHelper = OrderServiceHelper(limitOrderService, LOGGER)

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
                val feeInstruction = if (currentOrder.hasFee()) LimitOrderFeeInstruction.create(currentOrder.fee) else null
                val feeInstructions = NewLimitOrderFeeInstruction.create(currentOrder.feesList)
                orders.add(NewLimitOrder(uid, currentOrder.uid, message.assetPairId, message.clientId, currentOrder.volume,
                        currentOrder.price, OrderStatus.InOrderBook.name, Date(message.timestamp), now, currentOrder.volume, null,
                        fee = feeInstruction,
                        fees = listOfLimitOrderFee(feeInstruction, feeInstructions)))

                if (cancelAllPreviousLimitOrders) {
                    if (currentOrder.volume > 0) {
                        cancelBuySide = true
                    } else {
                        cancelSellSide = true
                    }
                }
            }
        }

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
        val ordersToAdd = LinkedList<NewLimitOrder>()
        val assetPair = assetsPairsHolder.getAssetPair(assetPairId)
        val balances = mutableMapOf(
                Pair(assetPair.baseAssetId, balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId)),
                Pair(assetPair.quotingAssetId, balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId)))

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)

        orders.forEach { order ->
            if (order.price <= 0) {
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to invalid price")
            } else if (!order.checkVolume(assetPair)) {
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due too small volume")
            } else if (orderBook.leadToNegativeSpreadForClient(order)) {
                LOGGER.info("[${order.assetPairId}] Unable to add order ${order.volume} @ ${order.price} due to negative spread")
            } else if (orderBook.leadToNegativeSpreadByOtherClient(order)) {
                val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()), balances[if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId])
                when (OrderStatus.valueOf(matchingResult.order.status)) {
                    OrderStatus.NoLiquidity -> {
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.NotEnoughFunds -> {
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.InvalidFee -> {
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.Matched,
                    OrderStatus.Processing-> {
                        val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
                        var cancelResult: CancelledOrdersOperationsResult? = null
                        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                            cancelResult = limitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.toList())
                            cancelledOrdersWalletOperations.addAll(cancelResult.walletOperations)
                        }
                        val preProcessUncompletedOrderResult = orderServiceHelper.preProcessUncompletedOrder(matchingResult, assetPair, cancelledOrdersWalletOperations)

                        val preProcessResult = try {
                            walletOperationsProcessor.preProcess(matchingResult.ownCashMovements).preProcess(matchingResult.oppositeCashMovements, true)
                            try {
                                walletOperationsProcessor.preProcess(cancelledOrdersWalletOperations)
                            } catch (e: BalanceException) {
                                LOGGER.error("[${order.assetPairId}] Unable to process cancelled orders wallet operations after matching: ${e.message}")
                            }
                            true
                        } catch (e: BalanceException) {
                            LOGGER.error("[${order.assetPairId}] Unable to process wallet operations after matching ${order.volume} @ ${order.price}: ${e.message}")
                            false
                        }

                        if (preProcessResult) {
                            matchingResult.apply()
                            limitOrderService.cancelLimitOrders(matchingResult.cancelledLimitOrders.toList())
                            orderServiceHelper.processUncompletedOrder(matchingResult, preProcessUncompletedOrderResult)

                            if (cancelResult != null) {
                                trustedClientLimitOrdersReport.orders.addAll(cancelResult.trustedClientLimitOrderWithTrades)
                                clientLimitOrdersReport.orders.addAll(cancelResult.clientLimitOrderWithTrades)
                            }

                            limitOrderService.moveOrdersToDone(matchingResult.completedLimitOrders)

                            cancelBuySide = cancelBuySide || matchingResult.cancelledLimitOrders.any { it.isBuySide() }
                            cancelSellSide = cancelSellSide || matchingResult.cancelledLimitOrders.any { !it.isBuySide() }

                            matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                            orderBook.setOrderBook(!order.isBuySide(), matchingResult.orderBook)

                            trades.addAll(matchingResult.lkkTrades)
                            var limitOrderWithTrades = clientLimitOrdersReport.orders.find { it.order.externalId == order.externalId }
                            if (limitOrderWithTrades == null) {
                                limitOrderWithTrades = LimitOrderWithTrades(order)
                                clientLimitOrdersReport.orders.add(limitOrderWithTrades)
                            }

                            limitOrderWithTrades.trades.addAll(matchingResult.marketOrderTrades.map { it ->
                                LimitTradeInfo(it.tradeId, it.marketClientId, it.marketAsset, it.marketVolume, it.price, matchingResult.timestamp, it.limitOrderId, it.limitOrderExternalId, it.limitAsset, it.limitClientId, it.limitVolume, it.feeInstruction, it.feeTransfer, it.fees, it.absoluteSpread, it.relativeSpread)
                            })

                            matchingResult.limitOrdersReport?.orders?.forEach { orderReport ->
                                var trustedOrder = clientLimitOrdersReport.orders.find { it.order.externalId == orderReport.order.externalId }
                                if (trustedOrder == null) {
                                    trustedOrder = LimitOrderWithTrades(orderReport.order)
                                    clientLimitOrdersReport.orders.add(trustedOrder)
                                }
                                trustedOrder.trades.addAll(orderReport.trades)
                            }

                            if (matchingResult.order.status == OrderStatus.Processing.name) {
                                if (assetPair.minVolume != null && order.getAbsRemainingVolume() < assetPair.minVolume) {
                                    LOGGER.info("Order (id: ${order.externalId}) is cancelled due to min remaining volume (${RoundingUtils.roundForPrint(order.getAbsRemainingVolume())} < ${RoundingUtils.roundForPrint(assetPair.minVolume)})")
                                    order.status = OrderStatus.Cancelled.name
                                } else {
                                    ordersToAdd.add(order)
                                    orderBook.addOrder(order)
                                }
                            }
                            balances[if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId] = matchingResult.marketBalance!!

                            sellSide = true
                            buySide = true
                        }
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
        }

        val startPersistTime = System.nanoTime()
        walletOperationsProcessor.apply(messageUid, MessageType.MULTI_LIMIT_ORDER.name)
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
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setMessageId(messageWrapper.messageId)
                    .setUid(messageUid.toLong())
                    .build())
        } else {
            val responseBuilder = ProtocolMessages.MultiLimitOrderResponse.newBuilder()
            responseBuilder.setId(messageUid).setStatus(MessageStatus.OK.type).setAssetPairId(assetPairId)
            orders.forEach {
                responseBuilder.addStatuses(ProtocolMessages.MultiLimitOrderResponse.OrderStatus.newBuilder().setId(it.externalId)
                        .setMatchingEngineId(it.id).setStatus(MessageStatus.OK.type).setVolume(it.volume).setPrice(it.price).build())
            }
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
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else  message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        } else {
            val message =  parseMultiLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else  message.uid
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_MULTI_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder()
                    .setMessageId(messageWrapper.messageId)
                    .setUid(messageWrapper.messageId!!.toLong())
                    .build())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setId(messageWrapper.messageId!!)
                    .setMessageId(messageWrapper.messageId)
                    .setStatus(status.type)
                    .build())
        }
    }
}
