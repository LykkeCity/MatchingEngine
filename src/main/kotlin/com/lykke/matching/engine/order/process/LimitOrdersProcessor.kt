package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.matching.MatchingResult
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.enums.TradeRole
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.CancelledOrdersOperationsResult
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.lang.IllegalArgumentException
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class LimitOrdersProcessor(private val isTrustedClient: Boolean,
                           private val baseAsset: Asset,
                           private val quotingAsset: Asset,
                           private val limitOrderInputValidator: LimitOrderInputValidator,
                           balancesHolder: BalancesHolder,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           private val applicationSettingsCache: ApplicationSettingsCache,
                           ordersToCancel: Collection<LimitOrder>,
                           private val clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                           private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                           private val trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                           private val matchingEngine: MatchingEngine,
                           private val date: Date,
                           private val clientId: String,
                           private val assetPair: AssetPair,
                           private val orderBook: AssetOrderBook,
                           payBackBaseReserved: BigDecimal,
                           payBackQuotingReserved: BigDecimal,
                           clientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
                           trustedClientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
                           private val businessValidator: LimitOrderBusinessValidator,
                           private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                           private val messageSender: MessageSender,
                           private val LOGGER: Logger) {

    private val orderServiceHelper = OrderServiceHelper(genericLimitOrderService, LOGGER)
    val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)

    private val availableBalances = HashMap<String, BigDecimal>()

    private var buySideOrderBookChanged = false
    private var sellSideOrderBookChanged = false

    private val ordersToCancel = ordersToCancel.filter { it.status != OrderStatus.Replaced.name }.toMutableList()
    private val completedOrders = ordersToCancel.filter { it.status == OrderStatus.Replaced.name }.toMutableList()

    private val processedOrders = mutableListOf<ProcessedOrder>()
    private val ordersToAdd = mutableListOf<LimitOrder>()

    private val lkkTrades = mutableListOf<LkkTrade>()
    private val clientsLimitOrdersWithTrades = toLimitOrdersWithTradesMap(clientsLimitOrdersWithTrades)
    private val trustedClientsLimitOrdersWithTrades = toLimitOrdersWithTradesMap(trustedClientsLimitOrdersWithTrades)

    init {
        if (orderBook.assetPairId != assetPair.assetPairId) {
            throw IllegalArgumentException("Invalid order book asset pair: ${orderBook.assetPairId}")
        }

        availableBalances[assetPair.baseAssetId] = NumberUtils.setScaleRoundHalfUp(balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId, payBackBaseReserved), baseAsset.accuracy)
        availableBalances[assetPair.quotingAssetId] = NumberUtils.setScaleRoundHalfUp(balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId, payBackQuotingReserved), quotingAsset.accuracy)
        val payBackReservedOperations = ArrayList<WalletOperation>(2)
        if (!isTrustedClient) {
            if (payBackBaseReserved > BigDecimal.ZERO) {
                payBackReservedOperations.add(WalletOperation(UUID.randomUUID().toString(), null, clientId, assetPair.baseAssetId, date, BigDecimal.ZERO, -payBackBaseReserved))
            }
            if (payBackQuotingReserved > BigDecimal.ZERO) {
                payBackReservedOperations.add(WalletOperation(UUID.randomUUID().toString(), null, clientId, assetPair.quotingAssetId, date, BigDecimal.ZERO, -payBackQuotingReserved))
            }
        }
        walletOperationsProcessor.preProcess(payBackReservedOperations, true)
    }

    private fun toLimitOrdersWithTradesMap(limitOrdersWithTrades: Collection<LimitOrderWithTrades>): MutableMap<String, LimitOrderWithTrades> {
        return limitOrdersWithTrades.groupBy { it.order.id }
                .mapValues { it.value.single() }
                .toMutableMap()
    }

    fun preProcess(messageId: String, orders: Collection<LimitOrder>): LimitOrdersProcessor {
        orders.forEach { preProcess(messageId, it) }
        return this
    }

    fun apply(messageId: String,
              processedMessage: ProcessedMessage?,
              operationId: String,
              messageType: MessageType,
              pBuySideOrderBookChanged: Boolean,
              pSellSideOrderBookChanged: Boolean): OrderProcessResult {

        buySideOrderBookChanged = buySideOrderBookChanged || pBuySideOrderBookChanged
        sellSideOrderBookChanged = sellSideOrderBookChanged || pSellSideOrderBookChanged

        val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
        val ordersToSave = mutableListOf<LimitOrder>()
        val ordersToRemove = mutableListOf<LimitOrder>()
        ordersToRemove.addAll(completedOrders)
        ordersToRemove.addAll(ordersToCancel)
        if (buySideOrderBookChanged) {
            val updatedOrders = matchingEngine.updatedOrders(orderBook.getCopyOfOrderBook(true), ordersToAdd)
            orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPair.assetPairId, true, updatedOrders.fullOrderBook))
            ordersToSave.addAll(updatedOrders.updatedOrders)
        }
        if (sellSideOrderBookChanged) {
            val updatedOrders = matchingEngine.updatedOrders(orderBook.getCopyOfOrderBook(false), ordersToAdd)
            orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPair.assetPairId, false, updatedOrders.fullOrderBook))
            ordersToSave.addAll(updatedOrders.updatedOrders)
        }

        var sequenceNumber: Long? = null
        var clientsSequenceNumber: Long? = null
        var trustedClientsSequenceNumber: Long? = null
        if (trustedClientsLimitOrdersWithTrades.isNotEmpty()) {
            trustedClientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = trustedClientsSequenceNumber
        }
        if (clientsLimitOrdersWithTrades.isNotEmpty()) {
            clientsSequenceNumber = messageSequenceNumberHolder.getNewValue()
            sequenceNumber = clientsSequenceNumber
        }

        val updated = walletOperationsProcessor.persistBalances(processedMessage,
                OrderBooksPersistenceData(orderBookPersistenceDataList, ordersToSave, ordersToRemove),
                null,
                sequenceNumber)
        if (!updated) {
            return OrderProcessResult(false, emptyList())
        }
        walletOperationsProcessor.apply().sendNotification(operationId, messageType.name, messageId)

        matchingEngine.apply()

        genericLimitOrderService.moveOrdersToDone(completedOrders)
        genericLimitOrderService.cancelLimitOrders(ordersToCancel, date)
        genericLimitOrderService.addOrders(ordersToAdd)
        genericLimitOrderService.setOrderBook(assetPair.assetPairId, orderBook)

        if (lkkTrades.isNotEmpty()) {
            lkkTradesQueue.put(lkkTrades)
        }

        val orderBookCopy = orderBook.copy()
        if (buySideOrderBookChanged) {
            genericLimitOrderService.putTradeInfo(TradeInfo(assetPair.assetPairId, true, orderBook.getBidPrice(), date))
            val newOrderBook = OrderBook(assetPair.assetPairId, true, date, orderBookCopy.getOrderBook(true))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }
        if (sellSideOrderBookChanged) {
            genericLimitOrderService.putTradeInfo(TradeInfo(assetPair.assetPairId, false, orderBook.getAskPrice(), date))
            val newOrderBook = OrderBook(assetPair.assetPairId, false, date, orderBookCopy.getOrderBook(false))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }

        if (trustedClientsLimitOrdersWithTrades.isNotEmpty()) {
            val trustedClientsLimitOrdersWithTrades = this.trustedClientsLimitOrdersWithTrades.values.toMutableList()
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(messageId, trustedClientsLimitOrdersWithTrades))
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(trustedClientsSequenceNumber!!,
                    messageId,
                    operationId,
                    date,
                    messageType,
                    trustedClientsLimitOrdersWithTrades))
        }

        if (clientsLimitOrdersWithTrades.isNotEmpty()) {
            val clientsLimitOrdersWithTrades = this.clientsLimitOrdersWithTrades.values.toMutableList()
            clientLimitOrdersQueue.put(LimitOrdersReport(messageId, clientsLimitOrdersWithTrades))
            messageSender.sendMessage(EventFactory.createExecutionEvent(clientsSequenceNumber!!,
                    messageId,
                    operationId,
                    date,
                    messageType,
                    walletOperationsProcessor.getClientBalanceUpdates(),
                    clientsLimitOrdersWithTrades))
        }

        return OrderProcessResult(true, processedOrders)
    }

    private fun preProcess(messageId: String, order: LimitOrder) {

        val limitAsset = if (order.isBuySide()) quotingAsset else baseAsset
        val limitVolume = if (order.isBuySide()) NumberUtils.setScaleRoundUp(order.getAbsVolume() * order.price, limitAsset.accuracy) else order.getAbsVolume()

        val orderInfo = orderInfo(order)
        val availableBalance = availableBalances[limitAsset.assetId]!!


        val orderValidationResult = validateLimitOrder(isTrustedClient, order, orderBook,
                assetPair, baseAsset, availableBalance, limitVolume)

        if (!orderValidationResult.isValid) {
            processInvalidOrder(orderValidationResult, order)
            return
        }

        if (orderBook.leadToNegativeSpread(order)) {
            val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()),
                    messageId,
                    availableBalance,
                    assetPair.limitOrderPriceDeviationThreshold ?: applicationSettingsCache.limitOrderPriceDeviationThreshold(assetPair.assetPairId))
            val orderCopy = matchingResult.order as LimitOrder
            val orderStatus = orderCopy.status
            when (OrderStatus.valueOf(orderStatus)) {
                OrderStatus.ReservedVolumeGreaterThanBalance -> {
                    processRejectedMatchingResult(matchingResult, order, "Reserved volume is higher than available balance")
                }
                OrderStatus.NoLiquidity,
                OrderStatus.NotEnoughFunds,
                OrderStatus.InvalidFee,
                OrderStatus.TooHighPriceDeviation -> {
                    processRejectedMatchingResult(matchingResult, order)
                }
                OrderStatus.InOrderBook,
                OrderStatus.Matched,
                OrderStatus.Processing -> {
                    if (!processMatchingResult(matchingResult, orderCopy, orderInfo, order, limitAsset)) return
                }
                else -> {
                    LOGGER.error("Not handled order status: ${matchingResult.order.status}")
                }
            }
            LOGGER.info("$orderInfo matched")
            return
        }

        if (order.timeInForce == OrderTimeInForce.IOC || order.timeInForce == OrderTimeInForce.FOK) {
            order.updateStatus(OrderStatus.Cancelled, date)
            LOGGER.info("$orderInfo: cancelled due to IOC")
            addToReport(order)
            processedOrders.add(ProcessedOrder(order, true))
            return
        }

        order.reservedLimitVolume = limitVolume
        orderBook.addOrder(order)
        ordersToAdd.add(order)
        addToReport(order.copy())
        processedOrders.add(ProcessedOrder(order, true))

        availableBalances[limitAsset.assetId] = availableBalance - limitVolume
        if (!isTrustedClient) {
            walletOperationsProcessor.preProcess(listOf(
                    WalletOperation(UUID.randomUUID().toString(), null, clientId, limitAsset.assetId, date, BigDecimal.ZERO, NumberUtils.setScaleRoundHalfUp(limitVolume, limitAsset.accuracy))
            ), true)
        }

        if (order.isBuySide()) {
            buySideOrderBookChanged = true
        } else {
            sellSideOrderBookChanged = true
        }

        if (!isTrustedClient) {
            LOGGER.info("$orderInfo added to order book")
        }
    }

    private fun processInvalidOrder(orderValidationResult: OrderValidationResult, order: LimitOrder) {
        LOGGER.info("Limit order (id: ${order.externalId}) is rejected: ${orderValidationResult.message}")
        order.updateStatus(orderValidationResult.status!!, date)
        addToReportIfNotTrusted(order)
        processedOrders.add(ProcessedOrder(order, false, orderValidationResult.message))
    }

    private fun processMatchingResult(matchingResult: MatchingResult, orderCopy: LimitOrder, orderInfo: String, order: LimitOrder, limitAsset: Asset): Boolean {
        val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
        val cancelResult = if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            val cancelResult = calculateCancelledOrdersOperations(matchingResult)
            cancelledOrdersWalletOperations.addAll(cancelResult.walletOperations)
            cancelResult
        } else null
        val preProcessUncompletedOrderResult = orderServiceHelper.preProcessUncompletedOrder(matchingResult, assetPair, cancelledOrdersWalletOperations)

        val ownWalletOperations = LinkedList<WalletOperation>(matchingResult.ownCashMovements)

        if (!processPartiallyMatchedOrder(matchingResult, orderCopy, orderInfo, order, limitAsset, ownWalletOperations)) {
            return false
        }

        try {
            walletOperationsProcessor.preProcess(ownWalletOperations).preProcess(matchingResult.oppositeCashMovements, true)
            preProcessCancelledOrdersWalletOperations(cancelledOrdersWalletOperations, order)
        } catch (e: BalanceException) {
            val message = "$orderInfo: Unable to process wallet operations after matching: ${e.message}"
            LOGGER.error(message)
            order.updateStatus(OrderStatus.NotEnoughFunds, matchingResult.timestamp)
            addToReportIfNotTrusted(order)
            processedOrders.add(ProcessedOrder(order, false, message))
            return false
        }

        matchingResult.apply()
        completedOrders.addAll(matchingResult.completedLimitOrders.map { it.origin!! })

        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            applyCancelledOrders(matchingResult, cancelResult!!)
        }

        matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }
        orderServiceHelper.processUncompletedOrder(matchingResult, preProcessUncompletedOrderResult, ordersToCancel)

        orderBook.setOrderBook(!order.isBuySide(), matchingResult.orderBook)
        lkkTrades.addAll(matchingResult.lkkTrades)

        val limitOrderWithTrades = LimitOrderWithTrades(order.copy(),
                matchingResult.marketOrderTrades.map { it ->
                    LimitTradeInfo(it.tradeId,
                            it.marketClientId,
                            it.marketAsset,
                            it.marketVolume,
                            it.price,
                            it.timestamp,
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
                }.toMutableList())
        clientsLimitOrdersWithTrades[limitOrderWithTrades.order.id] = limitOrderWithTrades

        matchingResult.limitOrdersReport?.orders?.forEach { addToReport(clientsLimitOrdersWithTrades, it) }

        if (OrderStatus.Processing.name == orderCopy.status || OrderStatus.InOrderBook.name == orderCopy.status) {
            orderBook.addOrder(order)
            ordersToAdd.add(order)
            if (order.isBuySide()) {
                buySideOrderBookChanged = true
            } else {
                sellSideOrderBookChanged = true
            }
        }

        availableBalances[limitAsset.assetId] = matchingResult.marketBalance!!

        if (order.isBuySide()) {
            sellSideOrderBookChanged = true
        } else {
            buySideOrderBookChanged = true
        }
        processedOrders.add(ProcessedOrder(order, true))
        return true
    }

    private fun processRejectedMatchingResult(matchingResult: MatchingResult, order: LimitOrder, reason: String? = null) {
        addToReportIfNotTrusted(order)
        processedOrders.add(ProcessedOrder(order, false, reason))
        processMatchingResultCancelledOrders(matchingResult, order)
    }

    private fun processMatchingResultCancelledOrders(matchingResult: MatchingResult, order: LimitOrder) {
        if (matchingResult.cancelledLimitOrders.isEmpty()) {
            return
        }
        val cancelResult = calculateCancelledOrdersOperations(matchingResult)
        preProcessCancelledOrdersWalletOperations(cancelResult.walletOperations, order)
        applyCancelledOrders(matchingResult, cancelResult)
        matchingResult.cancelledLimitOrders.forEach { orderBook.removeOrder(it.origin!!) }
    }

    private fun calculateCancelledOrdersOperations(matchingResult: MatchingResult): CancelledOrdersOperationsResult {
        return genericLimitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.map {
            val cancelledOrder = it.copy
            cancelledOrder.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
            cancelledOrder
        })
    }

    private fun preProcessCancelledOrdersWalletOperations(walletOperations: List<WalletOperation>, order: LimitOrder) {
        try {
            walletOperationsProcessor.preProcess(walletOperations)
        } catch (e: BalanceException) {
            LOGGER.error("${orderInfo(order)}: Unable to process cancelled orders wallet operations after matching: ${e.message}")
        }
    }

    private fun applyCancelledOrders(matchingResult: MatchingResult, cancelResult: CancelledOrdersOperationsResult) {
        if (matchingResult.cancelledLimitOrders.isEmpty()) {
            return
        }
        val originCancelledLimitOrders = matchingResult.cancelledLimitOrders.map { it.origin!! }
        ordersToCancel.addAll(originCancelledLimitOrders)
        cancelResult.trustedClientLimitOrderWithTrades.forEach { addToReport(trustedClientsLimitOrdersWithTrades, it) }
        cancelResult.clientLimitOrderWithTrades.forEach { addToReport(clientsLimitOrdersWithTrades, it) }
        buySideOrderBookChanged = buySideOrderBookChanged || originCancelledLimitOrders.any { it.isBuySide() }
        sellSideOrderBookChanged = sellSideOrderBookChanged || originCancelledLimitOrders.any { !it.isBuySide() }
    }

    private fun processPartiallyMatchedOrder(matchingResult: MatchingResult,
                                             orderCopy: LimitOrder,
                                             orderInfo: String,
                                             order: LimitOrder,
                                             limitAsset: Asset,
                                             ownWalletOperations: MutableCollection<WalletOperation>): Boolean {
        if (orderCopy.status != OrderStatus.Processing.name && orderCopy.status != OrderStatus.InOrderBook.name) {
            return true
        }
        when {
            assetPair.minVolume != null && orderCopy.getAbsRemainingVolume() < assetPair.minVolume -> {
                LOGGER.info("$orderInfo: cancelled due to min remaining volume (${NumberUtils.roundForPrint(orderCopy.getAbsRemainingVolume())} < ${NumberUtils.roundForPrint(assetPair.minVolume)})")
                orderCopy.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
            }
            matchingResult.matchedWithZeroLatestTrade -> {
                LOGGER.info("$orderInfo: cancelled due to zero latest trade")
                orderCopy.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
            }
            order.timeInForce == OrderTimeInForce.IOC -> {
                LOGGER.info("$orderInfo: cancelled after matching due to IOC, remainingVolume: ${orderCopy.remainingVolume}")
                orderCopy.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
            }
            order.timeInForce == OrderTimeInForce.FOK -> {
                LOGGER.info("$orderInfo: cancelled after matching due to FOK, remainingVolume: ${orderCopy.remainingVolume}")
                order.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
                addToReport(order)
                processedOrders.add(ProcessedOrder(order, true))
                return false
            }
            else -> {
                orderCopy.reservedLimitVolume = if (order.isBuySide()) NumberUtils.setScaleRoundDown(orderCopy.getAbsRemainingVolume() * orderCopy.price, limitAsset.accuracy) else orderCopy.getAbsRemainingVolume()
                if (!isTrustedClient) {
                    val newReservedBalance = NumberUtils.setScaleRoundHalfUp(orderCopy.reservedLimitVolume!!, limitAsset.accuracy)
                    ownWalletOperations.add(WalletOperation(UUID.randomUUID().toString(), null, orderCopy.clientId, limitAsset.assetId, matchingResult.timestamp, BigDecimal.ZERO, newReservedBalance))
                }
            }
        }
        return true
    }

    private fun validateLimitOrder(isTrustedClient: Boolean,
                                   order: LimitOrder,
                                   orderBook: AssetOrderBook,
                                   assetPair: AssetPair,
                                   baseAsset: Asset,
                                   availableBalance: BigDecimal,
                                   limitVolume: BigDecimal): OrderValidationResult {
        try {
            //input validator will be moved from the business thread after multilimit order context release
            limitOrderInputValidator.validateLimitOrder(isTrustedClient, order, assetPair, assetPair.assetPairId, baseAsset)
            businessValidator.performValidation(isTrustedClient, order, availableBalance, limitVolume, orderBook, date)
        } catch (e: OrderValidationException) {
            return OrderValidationResult(false, false, e.message, e.orderStatus)
        }

        return OrderValidationResult(true)
    }

    private fun orderInfo(order: LimitOrder) = "Limit order (id: ${order.externalId})"

    private fun addToReportIfNotTrusted(order: LimitOrder) {
        if (!isTrustedClient) {
            clientsLimitOrdersWithTrades[order.id] = LimitOrderWithTrades(order)
        }
    }

    private fun addToReport(order: LimitOrder) {
        if (isTrustedClient) {
            trustedClientsLimitOrdersWithTrades[order.id] = LimitOrderWithTrades(order)
        } else {
            clientsLimitOrdersWithTrades[order.id] = LimitOrderWithTrades(order)
        }
    }

    private fun addToReport(limitOrdersWithTrades: MutableMap<String, LimitOrderWithTrades>, limitOrderWithTrades: LimitOrderWithTrades) {
        if (limitOrdersWithTrades.containsKey(limitOrderWithTrades.order.id)) {
            limitOrdersWithTrades[limitOrderWithTrades.order.id]!!.trades.addAll(limitOrderWithTrades.trades)
        } else {
            limitOrdersWithTrades[limitOrderWithTrades.order.id] = limitOrderWithTrades
        }
    }

}