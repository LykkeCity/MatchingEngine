package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.MultiLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
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
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.services.validators.MultiLimitOrderValidator
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.enums.TradeRole
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.utils.MultiOrderFilter
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.ArrayList
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

@Service
class MultiLimitOrderService(private val limitOrderService: GenericLimitOrderService,
                             private val genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
                             private val limitOrdersProcessorFactory: LimitOrdersProcessorFactory,
                             private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                             private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                             private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                             private val orderBookQueue: BlockingQueue<OrderBook>,
                             private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                             private val assetsHolder: AssetsHolder,
                             private val assetsPairsHolder: AssetsPairsHolder,
                             private val balancesHolder: BalancesHolder,
                             genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory? = null,
                             private val multiLimitOrderValidator: MultiLimitOrderValidator,
                             feeProcessor: FeeProcessor,
                             private val settings: ApplicationSettingsCache,
                             private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                             private val messageSender: MessageSender) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${MultiLimitOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var ordersCount: Long = 0
    private var logCount = 1000
    private var totalPersistTime: Double = 0.0
    private var totalTime: Double = 0.0

    private val matchingEngine = MatchingEngine(LOGGER, limitOrderService, assetsHolder, assetsPairsHolder, feeProcessor)
    private val genericLimitOrderProcessor = genericLimitOrderProcessorFactory?.create(LOGGER)
    private val orderServiceHelper = OrderServiceHelper(limitOrderService, LOGGER)

    override fun processMessage(messageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()
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
        LOGGER.debug("Got old multi limit order messageId: ${messageWrapper.messageId}, id: $messageUid, client $clientId, assetPair: $assetPairId")
        cancelAllPreviousLimitOrders = message.cancelAllPreviousLimitOrders

        val assetPair = assetsPairsHolder.getAssetPair(assetPairId)
        val baseAssetAvailableBalance = balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId)
        val quotingAssetAvailableBalance = balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId)

        val filter = MultiOrderFilter(true,
                baseAssetAvailableBalance,
                quotingAssetAvailableBalance,
                assetsHolder.getAsset(assetPair.quotingAssetId).accuracy,
                now,
                message.ordersList.size,
                LOGGER)

        message.ordersList.forEach { currentOrder ->
            val uid = UUID.randomUUID().toString()
            val order = LimitOrder(uid, uid, message.assetPairId, message.clientId, BigDecimal.valueOf(currentOrder.volume),
                    BigDecimal.valueOf(currentOrder.price), OrderStatus.InOrderBook.name, now, Date(message.timestamp), now, BigDecimal.valueOf(currentOrder.volume), null,
                    type = LimitOrderType.LIMIT,
                    lowerLimitPrice = null,
                    lowerPrice = null,
                    upperLimitPrice = null,
                    upperPrice = null,
                    previousExternalId = null,
                    timeInForce = null,
                    expiryTime = null)

            filter.checkAndAdd(order)

            if (cancelAllPreviousLimitOrders) {
                if (currentOrder.volume > 0) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }

        filter.checkIfNotSorted()

        val orders = filter.getResult()
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
        val balances = mutableMapOf(
                Pair(assetPair.baseAssetId, baseAssetAvailableBalance),
                Pair(assetPair.quotingAssetId, quotingAssetAvailableBalance))

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)
        val completedOrders = mutableListOf<LimitOrder>()

        matchingEngine.initTransaction(walletOperationsProcessor)
        orders.forEach { order ->
            var orderValid = true
            try {
                multiLimitOrderValidator.performValidation(order, assetPair, orderBook)
            } catch (e: OrderValidationException) {
                orderValid = false
                order.updateStatus(e.orderStatus, now)
            }

            if (orderValid && orderBook.leadToNegativeSpreadByOtherClient(order)) {
                val matchingResult = matchingEngine.match(order,
                        orderBook.getOrderBook(!order.isBuySide()),
                        messageWrapper.messageId!!,
                        balances[if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId],
                        assetPair.limitOrderPriceDeviationThreshold ?: settings.limitOrderPriceDeviationThreshold(assetPairId))
                when (OrderStatus.valueOf(matchingResult.order.status)) {
                    OrderStatus.NoLiquidity -> {
                        order.updateStatus(OrderStatus.NoLiquidity, matchingResult.timestamp)
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.NotEnoughFunds -> {
                        order.updateStatus(OrderStatus.NotEnoughFunds, matchingResult.timestamp)
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.InvalidFee,
                    OrderStatus.TooHighPriceDeviation -> {
                        order.updateStatus(OrderStatus.InvalidFee, matchingResult.timestamp)
                        trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                    }
                    OrderStatus.Matched,
                    OrderStatus.Processing -> {
                        val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
                        var cancelResult: CancelledOrdersOperationsResult? = null
                        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                            cancelResult = limitOrderService.calculateWalletOperationsForCancelledOrders(
                                    matchingResult.cancelledLimitOrders.map {
                                        val cancelledOrder = it.copy
                                        cancelledOrder.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
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
                                        it.relativeSpread,
                                        TradeRole.TAKER,
                                        it.baseAssetId,
                                        it.baseVolume,
                                        it.quotingAssetId,
                                        it.quotingVolume)
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
                                    order.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
                                } else if (matchingResult.matchedWithZeroLatestTrade) {
                                    LOGGER.info("Order (id: ${order.externalId}) is cancelled due to zero latest trade")
                                    order.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
                                }  else {
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
            } else if (orderValid) {
                ordersToAdd.add(order)
                orderBook.addOrder(order)
                trustedClientLimitOrdersReport.orders.add(LimitOrderWithTrades(order))
                if (order.isBuySide()) buySide = true else sellSide = true
            }
        }

        val startPersistTime = System.nanoTime()

        val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
        val ordersToSave = mutableListOf<LimitOrder>()
        val ordersToRemove = mutableListOf<LimitOrder>()
        ordersToRemove.addAll(completedOrders)
        ordersToRemove.addAll(ordersToCancel)
        if (buySide || cancelBuySide) {
            val updatedOrders = matchingEngine.updatedOrders(orderBook.getCopyOfOrderBook(true), ordersToAdd)
            orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPairId, true, updatedOrders.fullOrderBook))
            ordersToSave.addAll(updatedOrders.updatedOrders)
        }
        if (sellSide || cancelSellSide) {
            val updatedOrders = matchingEngine.updatedOrders(orderBook.getCopyOfOrderBook(false), ordersToAdd)
            orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPairId, false, updatedOrders.fullOrderBook))
            ordersToSave.addAll(updatedOrders.updatedOrders)
        }

        var sequenceNumber: Long? = null
        var clientsSequenceNumber: Long? = null
        var trustedClientsSequenceNumber: Long? = null
        if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
            trustedClientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = trustedClientsSequenceNumber
        }
        if (clientLimitOrdersReport.orders.isNotEmpty()) {
            clientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = clientsSequenceNumber
        }

        val updated = walletOperationsProcessor.persistBalances(messageWrapper.processedMessage,
                OrderBooksPersistenceData(orderBookPersistenceDataList, ordersToSave, ordersToRemove),
                null,
                sequenceNumber)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            LOGGER.error("Unable to save result data (multi limit order id $messageUid)")
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
            return
        }

        walletOperationsProcessor.apply().sendNotification(messageUid, MessageType.MULTI_LIMIT_ORDER.name, messageWrapper.messageId!!)

        matchingEngine.apply()
        lkkTradesQueue.put(trades)
        limitOrderService.moveOrdersToDone(completedOrders)
        limitOrderService.cancelLimitOrders(ordersToCancel, now)
        limitOrderService.addOrders(ordersToAdd)
        limitOrderService.setOrderBook(assetPairId, orderBook)
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
                    " Persist: ${PrintUtils.convertToString(totalPersistTime)}, ${NumberUtils.roundForPrint2(100 * totalPersistTime / totalTime)} %")
            ordersCount = 0
            totalPersistTime = 0.0
            totalTime = 0.0
        }

        if (trustedClientLimitOrdersReport.orders.isNotEmpty()) {
            trustedClientsLimitOrdersQueue.put(trustedClientLimitOrdersReport)
            val outgoingMessage = EventFactory.createTrustedClientsExecutionEvent(trustedClientsSequenceNumber!!,
                    messageWrapper.messageId!!,
                    messageWrapper.id!!,
                    now,
                    MessageType.MULTI_LIMIT_ORDER,
                    trustedClientLimitOrdersReport.orders)
            messageSender.sendTrustedClientsMessage(outgoingMessage)
        }

        if (clientLimitOrdersReport.orders.isNotEmpty()) {
            clientLimitOrdersQueue.put(clientLimitOrdersReport)
            val outgoingMessage = EventFactory.createExecutionEvent(clientsSequenceNumber!!,
                    messageWrapper.messageId!!,
                    messageWrapper.id!!,
                    now,
                    MessageType.MULTI_LIMIT_ORDER,
                    walletOperationsProcessor.getClientBalanceUpdates(),
                    clientLimitOrdersReport.orders)
            messageSender.sendMessage(outgoingMessage)
        }

        genericLimitOrderProcessor?.checkAndProcessStopOrder(messageWrapper.messageId!!, assetPair, now)
    }

    private fun processMultiOrder(messageWrapper: MessageWrapper) {
        val message = messageWrapper.parsedMessage!! as ProtocolMessages.MultiLimitOrder
        val isTrustedClient = balancesHolder.isTrustedClient(message.clientId)

        LOGGER.debug("Got ${if (!isTrustedClient) "client " else ""}multi limit order id: ${message.uid}, " +
                (if (messageWrapper.messageId != message.uid) "messageId: ${messageWrapper.messageId}, " else "") +
                "client ${message.clientId}, " +
                "assetPair: ${message.assetPairId}, " +
                (if (message.hasCancelAllPreviousLimitOrders()) "cancelPrevious: ${message.cancelAllPreviousLimitOrders}, " else "") +
                (if (message.hasCancelMode()) "cancelMode: ${message.cancelMode}" else ""))

        val assetPair = assetsPairsHolder.getAssetPairAllowNulls(message.assetPairId)
        if (assetPair == null) {
            LOGGER.info("Unable to process message (${messageWrapper.messageId}: unknown asset pair")
            writeResponse(messageWrapper, MessageStatus.UNKNOWN_ASSET)
            return
        }
        val multiLimitOrder = readMultiLimitOrder(message, isTrustedClient, assetPair)
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

        val cancelResult = genericLimitOrdersCancellerFactory.create(LOGGER, now)
                .preProcessLimitOrders(ordersToCancel)
                .processLimitOrders()

        val orderBook = cancelResult.assetOrderBooks[multiLimitOrder.assetPairId] ?: limitOrderService.getOrderBook(multiLimitOrder.assetPairId).copy()

        val cancelBaseVolume = cancelResult.walletOperations
                .stream()
                .filter { it.assetId == assetPair.baseAssetId }
                .map({ t -> -t.reservedAmount })
                .reduce(BigDecimal.ZERO, BigDecimal::add)

        val cancelQuotingVolume = cancelResult.walletOperations
                .stream()
                .filter { it.assetId == assetPair.quotingAssetId }
                .map({ t -> -t.reservedAmount })
                .reduce(BigDecimal.ZERO, BigDecimal::add)

        notFoundReplacements.values.forEach {
            it.updateStatus(OrderStatus.NotFoundPrevious, now)
        }
        ordersToReplace.forEach {
            LOGGER.info("Order (${it.externalId}) is replaced by (${(multiLimitOrder.buyReplacements[it.externalId]
                    ?: multiLimitOrder.sellReplacements[it.externalId])?.externalId})")
            it.updateStatus(OrderStatus.Replaced, now)
        }

        val processor = limitOrdersProcessorFactory.create(matchingEngine,
                now,
                balancesHolder.isTrustedClient(multiLimitOrder.clientId),
                multiLimitOrder.clientId,
                assetPair,
                assetsHolder.getAsset(assetPair.baseAssetId),
                assetsHolder.getAsset(assetPair.quotingAssetId),
                orderBook,
                cancelBaseVolume,
                cancelQuotingVolume,
                ordersToCancel,
                cancelResult.clientsOrdersWithTrades,
                cancelResult.trustedClientsOrdersWithTrades,
                LOGGER)

        matchingEngine.initTransaction(processor.walletOperationsProcessor)
        val result = processor.preProcess(messageWrapper.messageId!!, multiLimitOrder.orders)
                .apply(messageWrapper.messageId!!,
                        messageWrapper.processedMessage,
                        multiLimitOrder.messageUid, MessageType.MULTI_LIMIT_ORDER,
                        buySideOrderBookChanged, sellSideOrderBookChanged)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = result.success
        val responseBuilder = ProtocolMessages.MultiLimitOrderResponse.newBuilder()

        if (!result.success) {
            val errorMessage = "Unable to save result data"
            LOGGER.error("$errorMessage (multi limit order id ${multiLimitOrder.messageUid})")

            messageWrapper.writeMultiLimitOrderResponse(responseBuilder
                    .setStatus(MessageStatus.RUNTIME.type)
                    .setAssetPairId(multiLimitOrder.assetPairId)
                    .setStatusReason(errorMessage))

            return
        }

        responseBuilder.setId(multiLimitOrder.messageUid)
                .setStatus(MessageStatus.OK.type).assetPairId = multiLimitOrder.assetPairId

        result.orders.forEach { processedOrder ->
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

        genericLimitOrderProcessor?.checkAndProcessStopOrder(messageWrapper.messageId!!, assetPair, now)
    }

    private fun readMultiLimitOrder(message: ProtocolMessages.MultiLimitOrder,
                                    isTrustedClient: Boolean,
                                    assetPair: AssetPair): MultiLimitOrder {
        val clientId = message.clientId
        val messageUid = message.uid
        val assetPairId = message.assetPairId
        val cancelAllPreviousLimitOrders = message.cancelAllPreviousLimitOrders
        val cancelMode = if (message.hasCancelMode()) OrderCancelMode.getByExternalId(message.cancelMode) else OrderCancelMode.NOT_EMPTY_SIDE
        val now = Date()
        var cancelBuySide = cancelMode == OrderCancelMode.BUY_SIDE || cancelMode == OrderCancelMode.BOTH_SIDES
        var cancelSellSide = cancelMode == OrderCancelMode.SELL_SIDE || cancelMode == OrderCancelMode.BOTH_SIDES

        val buyReplacements = mutableMapOf<String, LimitOrder>()
        val sellReplacements = mutableMapOf<String, LimitOrder>()

        val baseAssetAvailableBalance = balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId)
        val quotingAssetAvailableBalance = balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId)

        val filter = MultiOrderFilter(isTrustedClient,
                baseAssetAvailableBalance,
                quotingAssetAvailableBalance,
                assetsHolder.getAsset(assetPair.quotingAssetId).accuracy,
                now,
                message.ordersList.size,
                LOGGER)

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
                    now,
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
                    previousExternalId = previousExternalId,
                    timeInForce = if (currentOrder.hasTimeInForce()) OrderTimeInForce.getByExternalId(currentOrder.timeInForce) else null,
                    expiryTime = if (currentOrder.hasExpiryTime()) Date(currentOrder.expiryTime) else null)

            filter.checkAndAdd(order)
            previousExternalId?.let {
                (if (order.isBuySide()) buyReplacements else sellReplacements)[it] = order
            }

            if (cancelAllPreviousLimitOrders && cancelMode == OrderCancelMode.NOT_EMPTY_SIDE) {
                if (currentOrder.volume > 0) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }

        filter.checkIfNotSorted()

        return MultiLimitOrder(messageUid,
                clientId,
                assetPairId,
                filter.getResult(),
                cancelAllPreviousLimitOrders,
                cancelBuySide,
                cancelSellSide,
                cancelMode,
                buyReplacements,
                sellReplacements)
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
                val newOrder = mutableReplacements.remove(it.externalId)
                if (!isAlreadyCancelled) {
                    ordersToCancel.add(it)
                    addedToCancel = true
                }
                if (newOrder?.status == OrderStatus.InOrderBook.name) {
                    ordersToReplace.add(it)
                }
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
            val message = parseOldMultiLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid.toString()
        } else {
            val message = parseMultiLimitOrder(messageWrapper.byteArray)
            messageWrapper.messageId = if (message.hasMessageId()) message.messageId else message.uid.toString()
            messageWrapper.timestamp = message.timestamp
            messageWrapper.parsedMessage = message
            messageWrapper.id = message.uid
            messageWrapper.processedMessage = if (settings.isTrustedClient(message.clientId))
                null
            else
                ProcessedMessage(messageWrapper.type, messageWrapper.timestamp!!, messageWrapper.messageId!!)
        }
    }

    override fun writeResponse(messageWrapper: MessageWrapper, status: MessageStatus) {
        if (messageWrapper.type == MessageType.OLD_MULTI_LIMIT_ORDER.type) {
            messageWrapper.writeResponse(ProtocolMessages.Response.newBuilder())
        } else {
            val assetPairId = (messageWrapper.parsedMessage as ProtocolMessages.MultiLimitOrder).assetPairId
            messageWrapper.writeMultiLimitOrderResponse(ProtocolMessages.MultiLimitOrderResponse.newBuilder()
                    .setStatus(status.type).setAssetPairId(assetPairId))
        }
    }
}