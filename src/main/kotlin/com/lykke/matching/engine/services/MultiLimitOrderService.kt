package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.fee.listOfLimitOrderFee
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.services.validators.MultiLimitOrderValidator
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.ArrayList
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class MultiLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                             private val genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
                             private val limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
                             private val trustedClientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                             private val clientLimitOrderReportQueue: BlockingQueue<JsonSerializable>,
                             private val orderBookQueue: BlockingQueue<OrderBook>,
                             private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                             assetsHolder: AssetsHolder,
                             private val assetsPairsHolder: AssetsPairsHolder,
                             private val balancesHolder: BalancesHolder,
                             private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                             genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory?= null,
                             private val multiLimitOrderValidator: MultiLimitOrderValidator): AbstractService {

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
    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory?.create(LOGGER)
    private val orderServiceHelper = OrderServiceHelper(limitOrderService, LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
        val orders: List<LimitOrder>
        val now = Date()
        var cancelBuySide = false
        var cancelSellSide = false
        val cancelAllPreviousLimitOrders: Boolean
        val messageUid: String
        val clientId: String
        val assetPairId: String

        if (messageWrapper.parsedMessage == null) {
            parseMessage(messageWrapper)
        }
        if (messageWrapper.type != MessageType.OLD_MULTI_LIMIT_ORDER.type) {
            processMultiOrder(messageWrapper)
            return
        }
        val trustedClientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
        val clientLimitOrdersReport = LimitOrdersReport(messageWrapper.messageId!!)
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.OldMultiLimitOrder
        messageUid = message.uid.toString()
        clientId = message.clientId
        assetPairId = message.assetPairId
        LOGGER.debug("Got old multi limit order  messageId: ${messageWrapper.messageId}, id: $messageUid, client $clientId, assetPair: $assetPairId")
        orders = ArrayList(message.ordersList.size)
        cancelAllPreviousLimitOrders = message.cancelAllPreviousLimitOrders
        message.ordersList.forEach { currentOrder ->
            val uid = UUID.randomUUID().toString()
            orders.add(LimitOrder(uid, uid, message.assetPairId, message.clientId, BigDecimal.valueOf(currentOrder.volume),
                    BigDecimal.valueOf(currentOrder.price), OrderStatus.InOrderBook.name, Date(message.timestamp), now, BigDecimal.valueOf(currentOrder.volume), null,
                    type = LimitOrderType.LIMIT,
                    lowerLimitPrice = null,
                    lowerPrice = null,
                    upperLimitPrice = null,
                    upperPrice = null,
                    previousExternalId = null))

            if (cancelAllPreviousLimitOrders) {
                if (currentOrder.volume > 0) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }



        val ordersToCancel = ArrayList<LimitOrder>()

        if (cancelAllPreviousLimitOrders) {
            if (cancelBuySide) {
                ordersToCancel.addAll(limitOrderService.searchOrders(clientId, assetPairId, true))
            }
            if (cancelSellSide) {
                ordersToCancel.addAll(limitOrderService.searchOrders(clientId, assetPairId, false))
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
        val ordersToAdd = LinkedList<LimitOrder>()
        val assetPair = assetsPairsHolder.getAssetPair(assetPairId)
        val balances = mutableMapOf(
                Pair(assetPair.baseAssetId, balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId)),
                Pair(assetPair.quotingAssetId, balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId)))

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)
        val completedOrders = mutableListOf<LimitOrder>()

        matchingEngine.initTransaction()
        orders.forEach { order ->
            var orderValid = true
            try {
                multiLimitOrderValidator.performValidation(order, assetPair, orderBook)
            } catch (e: OrderValidationException) {
                orderValid = false
                order.status = e.orderStatus.name
            }

            if (orderValid && orderBook.leadToNegativeSpreadByOtherClient(order)) {
                val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()), messageWrapper.messageId!!,
                        balances[if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId])
                when (OrderStatus.valueOf(matchingResult.order.status)) {
                    OrderStatus.NoLiquidity -> {
                        order.status = OrderStatus.NoLiquidity.name
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.NotEnoughFunds -> {
                        order.status = OrderStatus.NotEnoughFunds.name
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.InvalidFee -> {
                        order.status = OrderStatus.InvalidFee.name
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.Matched,
                    OrderStatus.Processing-> {
                        val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
                        var cancelResult: CancelledOrdersOperationsResult? = null
                        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                            cancelResult = limitOrderService.calculateWalletOperationsForCancelledOrders(
                                    matchingResult.cancelledLimitOrders.map {
                                        val cancelledOrder = it.copy
                                        cancelledOrder.status = OrderStatus.Cancelled.name
                                        cancelledOrder
                                    })
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
                            ordersToCancel.addAll(matchingResult.cancelledLimitOrders.map { it.origin!! })
                            orderServiceHelper.processUncompletedOrder(matchingResult, preProcessUncompletedOrderResult, ordersToCancel)

                            if (cancelResult != null) {
                                trustedClientLimitOrdersReport.orders.addAll(cancelResult.trustedClientLimitOrderWithTrades)
                                clientLimitOrdersReport.orders.addAll(cancelResult.clientLimitOrderWithTrades)
                            }

                            completedOrders.addAll(matchingResult.completedLimitOrders.map { it.origin!! })

                            cancelBuySide = cancelBuySide || ordersToCancel.any { it.isBuySide() }
                            cancelSellSide = cancelSellSide || ordersToCancel.any { !it.isBuySide() }

                            matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                            orderBook.setOrderBook(!order.isBuySide(), matchingResult.orderBook)

                            trades.addAll(matchingResult.lkkTrades)
                            var limitOrderWithTrades = clientLimitOrdersReport.orders.find { it.order.id == order.id }
                            if (limitOrderWithTrades == null) {
                                limitOrderWithTrades = LimitOrderWithTrades(order)
                                clientLimitOrdersReport.orders.add(limitOrderWithTrades)
                            }

                            limitOrderWithTrades.trades.addAll(matchingResult.marketOrderTrades.map { it ->
                                LimitTradeInfo(it.tradeId,
                                        it.marketClientId,
                                        it.marketAsset,
                                        it.marketVolume,
                                        it.price,
                                        matchingResult.timestamp,
                                        it.limitOrderId,
                                        it.limitOrderExternalId,
                                        it.limitAsset,
                                        it.limitClientId,
                                        it.limitVolume,
                                        it.index,
                                        it.feeInstruction,
                                        it.feeTransfer,
                                        it.fees,
                                        it.absoluteSpread,
                                        it.relativeSpread)
                            })

                            matchingResult.limitOrdersReport?.orders?.forEach { orderReport ->
                                var trustedOrder = clientLimitOrdersReport.orders.find { it.order.id == orderReport.order.id }
                                if (trustedOrder == null) {
                                    trustedOrder = LimitOrderWithTrades(orderReport.order)
                                    clientLimitOrdersReport.orders.add(trustedOrder)
                                }
                                trustedOrder.trades.addAll(orderReport.trades)
                            }

                            if (matchingResult.order.status == OrderStatus.Processing.name) {
                                if (assetPair.minVolume != null && order.getAbsRemainingVolume() < assetPair.minVolume) {
                                    LOGGER.info("Order (id: ${order.externalId}) is cancelled due to min remaining volume (${NumberUtils.roundForPrint(order.getAbsRemainingVolume())} < ${NumberUtils.roundForPrint(assetPair.minVolume)})")
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
            } else if(orderValid) {
                ordersToAdd.add(order)
                orderBook.addOrder(order)
                trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                if (order.isBuySide()) buySide = true else sellSide = true
            }
        }

        val startPersistTime = System.nanoTime()

        val updated = walletOperationsProcessor.persistBalances()
        if (!updated) {
            LOGGER.error("Unable to save result data (multi limit order id $messageUid)")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
            return
        }

        walletOperationsProcessor.apply().sendNotification(messageUid, MessageType.MULTI_LIMIT_ORDER.name, messageWrapper.messageId!!)

        matchingEngine.apply()
        lkkTradesQueue.put(trades)
        limitOrderService.moveOrdersToDone(completedOrders)
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

        messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())

        val endTime = System.nanoTime()

        messagesCount++
        ordersCount += orders.size
        totalPersistTime += (endPersistTime - startPersistTime) / logCount.toLong()
        totalTime += (endTime - startTime) / logCount.toLong()

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Orders: $ordersCount/$logCount messages. Total: ${PrintUtils.convertToString(totalTime)}. " +
                    " Persist: ${PrintUtils.convertToString(totalPersistTime)}, ${NumberUtils.roundForPrint2(100*totalPersistTime/totalTime)} %")
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

        genericLimitOrderProcessor?.checkAndProcessStopOrder(messageWrapper.messageId!!, assetPair.assetPairId, now)
    }
    private fun processMultiOrder(messageWrapper: MessageWrapper) {
        val multiLimitOrder = readMultiLimitOrder(messageWrapper.parsedMessage!! as ProtocolMessages.MultiLimitOrder)
        val isTrustedClient = balancesHolder.isTrustedClient(multiLimitOrder.clientId)
        if (isTrustedClient) {
            LOGGER.debug("Got multi limit order id: ${multiLimitOrder.messageUid}, client ${multiLimitOrder.clientId}, assetPair: ${multiLimitOrder.assetPairId}")
        } else {
            LOGGER.debug("Got client multi limit order id: ${multiLimitOrder.messageUid}, client ${multiLimitOrder.clientId}, assetPair: ${multiLimitOrder.assetPairId}, cancelPrevious: ${multiLimitOrder.cancelAllPreviousLimitOrders}, cancelMode: ${multiLimitOrder.cancelMode}")
        }
        val now = Date()

        var buySideOrderBookChanged = false
        var sellSideOrderBookChanged = false

        var previousBuyOrders: Collection<LimitOrder>? = null
        var previousSellOrders: Collection<LimitOrder>? = null
        val ordersToReplace = mutableListOf<LimitOrder>()

        val ordersToCancel = ArrayList<LimitOrder>()
        if (multiLimitOrder.cancelAllPreviousLimitOrders) {
            if (multiLimitOrder.cancelBuySide) {
                previousBuyOrders = limitOrderService.searchOrders(multiLimitOrder.clientId, multiLimitOrder.assetPairId, true)
                ordersToCancel.addAll(previousBuyOrders)
                buySideOrderBookChanged = true
            }
            if (multiLimitOrder.cancelSellSide) {
                previousSellOrders = limitOrderService.searchOrders(multiLimitOrder.clientId, multiLimitOrder.assetPairId, false)
                ordersToCancel.addAll(previousSellOrders)
                sellSideOrderBookChanged = true
            }
        }



        val notFoundReplacements = mutableMapOf<String, LimitOrder>()

        buySideOrderBookChanged = processReplacements(multiLimitOrder,
                true,
                notFoundReplacements,
                previousBuyOrders,
                ordersToCancel,
                ordersToReplace) || buySideOrderBookChanged

        sellSideOrderBookChanged = processReplacements(multiLimitOrder,
                false,
                notFoundReplacements,
                previousSellOrders,
                ordersToCancel,
                ordersToReplace) || sellSideOrderBookChanged


        val assetPair = assetsPairsHolder.getAssetPair(multiLimitOrder.assetPairId)
        val cancelResult = genericLimitOrdersCancellerFactory.create(LOGGER, now)
                .preProcessLimitOrders(ordersToCancel)
                .processLimitOrders()

        val orderBook = cancelResult.assetOrderBooks[multiLimitOrder.assetPairId] ?: limitOrderService.getOrderBook(multiLimitOrder.assetPairId).copy()

        val cancelBaseVolume = cancelResult.walletOperations
                .stream()
                .filter { it.assetId == assetPair.baseAssetId }
                .map({ t ->  -t.reservedAmount })
                .reduce(BigDecimal.ZERO, BigDecimal::add)

        val cancelQuotingVolume = cancelResult.walletOperations
                .stream()
                .filter { it.assetId == assetPair.quotingAssetId }
                .map({ t ->  -t.reservedAmount})
                .reduce(BigDecimal.ZERO, BigDecimal::add)

        notFoundReplacements.values.forEach {
            it.status = OrderStatus.NotFoundPrevious.name
        }
        ordersToReplace.forEach {
            LOGGER.info("Order (${it.externalId}) is replaced by (${(multiLimitOrder.buyReplacements[it.externalId]
                    ?: multiLimitOrder.sellReplacements[it.externalId])?.externalId})")
            it.status = OrderStatus.Replaced.name
        }

        val processor = limitOrdersProcessorFactory.create(matchingEngine,
                now,
                multiLimitOrder.clientId,
                assetPair,
                orderBook,
                cancelBaseVolume,
                cancelQuotingVolume,
                ordersToCancel,
                cancelResult.clientsOrdersWithTrades,
                cancelResult.trustedClientsOrdersWithTrades,
                LOGGER)

        matchingEngine.initTransaction()
        val result = processor.preProcess(messageWrapper.messageId!!, multiLimitOrder.orders)
                .apply(messageWrapper.messageId!!, multiLimitOrder.messageUid, MessageType.MULTI_LIMIT_ORDER.name,
                        buySideOrderBookChanged, sellSideOrderBookChanged)

        val responseBuilder = ProtocolMessages.MultiLimitOrderResponse.newBuilder()

        if (!result.success) {
            val message = "Unable to save result data"
            LOGGER.error("$message (multi limit order id ${multiLimitOrder.messageUid})")

            messageWrapper.writeMultiLimitOrderResponse(responseBuilder
                    .setStatus(MessageStatus.RUNTIME.type)
                    .setAssetPairId(multiLimitOrder.assetPairId)
                    .setStatusReason(message))

            return
        }

        responseBuilder.setId(multiLimitOrder.messageUid)
                .setStatus(MessageStatus.OK.type).assetPairId = multiLimitOrder.assetPairId

        result.orders.forEach {processedOrder ->
            val order = processedOrder.order
            val statusBuilder = ProtocolMessages.MultiLimitOrderResponse.OrderStatus.newBuilder()
                    .setId(order.externalId)
                    .setMatchingEngineId(order.id)
                    .setStatus(MessageStatusUtils.toMessageStatus(order.status).type)
                    .setVolume(order.volume.toDouble())
                    .setPrice(order.price.toDouble())
            processedOrder.reason?.let { statusBuilder.statusReason = processedOrder.reason }
            responseBuilder.addStatuses(statusBuilder.build())
        }
        messageWrapper.writeMultiLimitOrderResponse(responseBuilder)

        genericLimitOrderProcessor?.checkAndProcessStopOrder(messageWrapper.messageId!!, assetPair.assetPairId, now)
    }

    private fun readMultiLimitOrder(message: ProtocolMessages.MultiLimitOrder): MultiLimitOrder {
        val clientId = message.clientId
        val messageUid = message.uid
        val assetPairId = message.assetPairId
        val cancelAllPreviousLimitOrders = message.cancelAllPreviousLimitOrders
        val cancelMode = if (message.hasCancelMode()) OrderCancelMode.getByExternalId(message.cancelMode) else OrderCancelMode.NOT_EMPTY_SIDE
        val orders = ArrayList<LimitOrder>(message.ordersList.size)
        val now = Date()
        var cancelBuySide = cancelMode == OrderCancelMode.BUY_SIDE || cancelMode == OrderCancelMode.BOTH_SIDES
        var cancelSellSide = cancelMode == OrderCancelMode.SELL_SIDE || cancelMode == OrderCancelMode.BOTH_SIDES

        val buyReplacements = mutableMapOf<String, LimitOrder>()
        val sellReplacements = mutableMapOf<String, LimitOrder>()

        message.ordersList.forEach { currentOrder ->

            val feeInstruction = if (currentOrder.hasFee()) LimitOrderFeeInstruction.create(currentOrder.fee) else null
            val feeInstructions = NewLimitOrderFeeInstruction.create(currentOrder.feesList)
            val previousExternalId = if (currentOrder.hasOldUid()) currentOrder.oldUid else null

            val order = LimitOrder(UUID.randomUUID().toString(),
                    currentOrder.uid,
                    message.assetPairId,
                    message.clientId,
                    BigDecimal.valueOf(currentOrder.volume),
                    BigDecimal.valueOf(currentOrder.price),
                    OrderStatus.InOrderBook.name,
                    Date(message.timestamp),
                    now,
                    BigDecimal.valueOf(currentOrder.volume),
                    null,
                    fee = feeInstruction,
                    fees = listOfLimitOrderFee(feeInstruction, feeInstructions),
                    type = LimitOrderType.LIMIT,
                    upperPrice = null,
                    upperLimitPrice = null,
                    lowerPrice = null,
                    lowerLimitPrice = null,
                    previousExternalId = previousExternalId)

            previousExternalId?.let {
                (if (order.isBuySide()) buyReplacements else sellReplacements).put(it, order)
            }

            orders.add(order)

            if (cancelAllPreviousLimitOrders && cancelMode == OrderCancelMode.NOT_EMPTY_SIDE) {
                if (currentOrder.volume > 0) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }

        return MultiLimitOrder(messageUid, clientId, assetPairId, orders, cancelAllPreviousLimitOrders, cancelBuySide, cancelSellSide, cancelMode, buyReplacements, sellReplacements)
    }

    private fun processReplacements(multiLimitOrder: MultiLimitOrder,
                                    isBuy: Boolean,
                                    notFoundReplacements: MutableMap<String, LimitOrder>,
                                    previousOrders: Collection<LimitOrder>?,
                                    ordersToCancel: MutableCollection<LimitOrder>,
                                    ordersToReplace: MutableCollection<LimitOrder>): Boolean {
        var addedToCancel = false
        val replacements = if (isBuy) multiLimitOrder.buyReplacements else multiLimitOrder.sellReplacements
        if (replacements.isEmpty()) {
            return addedToCancel
        }
        val mutableReplacements = replacements.toMutableMap()
        val isAlreadyCancelled = isBuy && multiLimitOrder.cancelBuySide || !isBuy && multiLimitOrder.cancelSellSide
        val ordersToCheck = previousOrders ?: limitOrderService.searchOrders(multiLimitOrder.clientId, multiLimitOrder.assetPairId, isBuy)
        ordersToCheck.forEach {
            if (mutableReplacements.containsKey(it.externalId)) {
                mutableReplacements.remove(it.externalId)
                if (!isAlreadyCancelled) {
                    ordersToCancel.add(it)
                    addedToCancel = true
                }
                ordersToReplace.add(it)
            }
        }
        notFoundReplacements.putAll(mutableReplacements)
        return addedToCancel
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
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid.toString()
        } else {
            val message =  parseMultiLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = if(message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_MULTI_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
        } else {
            messageWrapper.writeNewResponse(ProtocolMessages.NewResponse.newBuilder()
                    .setStatus(status.type))
        }
    }
}