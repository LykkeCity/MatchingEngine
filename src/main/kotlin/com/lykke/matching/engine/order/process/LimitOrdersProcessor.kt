package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.matching.MatchingResult
import com.lykke.matching.engine.order.LimitOrderValidator
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.utils.OrderServiceHelper
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import java.lang.IllegalArgumentException
import java.math.BigDecimal
import java.util.Date
import java.util.LinkedList
import java.util.UUID
import java.util.concurrent.BlockingQueue

class LimitOrdersProcessor(assetsHolder: AssetsHolder,
                           assetsPairsHolder: AssetsPairsHolder,
                           balancesHolder: BalancesHolder,
                           private val genericLimitOrderService: GenericLimitOrderService,
                           applicationSettingsCache: ApplicationSettingsCache,
                           ordersToCancel: Collection<NewLimitOrder>,
                           private val trustedClientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                           private val clientsLimitOrdersQueue: BlockingQueue<JsonSerializable>,
                           private val lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                           private val orderBookQueue: BlockingQueue<OrderBook>,
                           private val rabbitOrderBookQueue: BlockingQueue<JsonSerializable>,
                           private val matchingEngine: MatchingEngine,
                           private val date: Date,
                           private val clientId: String,
                           private val assetPair: AssetPair,
                           private val orderBook: AssetOrderBook,
                           payBackBaseReserved: Double,
                           payBackQuotingReserved: Double,
                           clientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
                           trustedClientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
                           private val LOGGER: Logger) {

    private val validator = LimitOrderValidator(assetsPairsHolder, assetsHolder, applicationSettingsCache)

    private val orderServiceHelper = OrderServiceHelper(genericLimitOrderService, LOGGER)
    private val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)

    private val isTrustedClient = balancesHolder.isTrustedClient(clientId)
    private val baseAsset = assetsHolder.getAsset(assetPair.baseAssetId)
    private val quotingAsset = assetsHolder.getAsset(assetPair.quotingAssetId)
    private val availableBalances = HashMap<String, BigDecimal>()

    private var buySideOrderBookChanged = false
    private var sellSideOrderBookChanged = false

    private val ordersToCancel = ordersToCancel.filter { it.status != OrderStatus.Replaced.name }.toMutableList()
    private val completedOrders = ordersToCancel.filter { it.status == OrderStatus.Replaced.name }.toMutableList()

    private val processedOrders = mutableListOf<ProcessedOrder>()
    private val ordersToAdd = mutableListOf<NewLimitOrder>()

    private val lkkTrades = mutableListOf<LkkTrade>()
    private val clientsLimitOrdersWithTrades = clientsLimitOrdersWithTrades.toMutableList()
    private val trustedClientsLimitOrdersWithTrades = trustedClientsLimitOrdersWithTrades.toMutableList()

    init {
        if (orderBook.assetId != assetPair.assetPairId) {
            throw IllegalArgumentException("Invalid order book asset pair: ${orderBook.assetId}")
        }

        availableBalances[assetPair.baseAssetId] = NumberUtils.parseDouble(balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId, payBackBaseReserved), baseAsset.accuracy)
        availableBalances[assetPair.quotingAssetId] = NumberUtils.parseDouble(balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId, payBackQuotingReserved), quotingAsset.accuracy)
        val payBackReservedOperations = ArrayList<WalletOperation>(2)
        if (!isTrustedClient) {
            if (payBackBaseReserved > 0) {
                payBackReservedOperations.add(WalletOperation(UUID.randomUUID().toString(), null, clientId, assetPair.baseAssetId, date, 0.0, -payBackBaseReserved))
            }
            if (payBackQuotingReserved > 0) {
                payBackReservedOperations.add(WalletOperation(UUID.randomUUID().toString(), null, clientId, assetPair.quotingAssetId, date, 0.0, -payBackQuotingReserved))
            }
        }
        walletOperationsProcessor.preProcess(payBackReservedOperations, true)
    }

    fun preProcess(messageId: String, orders: Collection<NewLimitOrder>): LimitOrdersProcessor {
        orders.forEach { preProcess(messageId, it) }
        return this
    }

    fun apply(messageId: String,
              operationId: String,
              operationType: String,
              pBuySideOrderBookChanged: Boolean,
              pSellSideOrderBookChanged: Boolean): OrderProcessResult {

        buySideOrderBookChanged = buySideOrderBookChanged || pBuySideOrderBookChanged
        sellSideOrderBookChanged = sellSideOrderBookChanged || pSellSideOrderBookChanged

        val updated = walletOperationsProcessor.persistBalances()
        if (!updated) {
            return OrderProcessResult(false, emptyList())
        }
        walletOperationsProcessor.apply().sendNotification(operationId, operationType, messageId)

        matchingEngine.apply()

        genericLimitOrderService.moveOrdersToDone(completedOrders)
        genericLimitOrderService.cancelLimitOrders(ordersToCancel)
        genericLimitOrderService.addOrders(ordersToAdd)
        genericLimitOrderService.setOrderBook(assetPair.assetPairId, orderBook)

        if (lkkTrades.isNotEmpty()) {
            lkkTradesQueue.put(lkkTrades)
        }

        val orderBookCopy = orderBook.copy()
        if (buySideOrderBookChanged) {
            genericLimitOrderService.updateOrderBook(assetPair.assetPairId, true)
            val newOrderBook = OrderBook(assetPair.assetPairId, true, date, orderBookCopy.getOrderBook(true))
            genericLimitOrderService.putTradeInfo(TradeInfo(assetPair.assetPairId, true, orderBookCopy.getBidPrice(), date))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }
        if (sellSideOrderBookChanged) {
            genericLimitOrderService.updateOrderBook(assetPair.assetPairId, false)
            val newOrderBook = OrderBook(assetPair.assetPairId, false, date, orderBookCopy.getOrderBook(false))
            genericLimitOrderService.putTradeInfo(TradeInfo(assetPair.assetPairId, false, orderBookCopy.getAskPrice(), date))
            orderBookQueue.put(newOrderBook)
            rabbitOrderBookQueue.put(newOrderBook)
        }

        if (trustedClientsLimitOrdersWithTrades.isNotEmpty()) {
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(messageId, trustedClientsLimitOrdersWithTrades))
        }

        if (clientsLimitOrdersWithTrades.isNotEmpty()) {
            clientsLimitOrdersQueue.put(LimitOrdersReport(messageId, clientsLimitOrdersWithTrades))
        }

        return OrderProcessResult(true, processedOrders)
    }

    private fun preProcess(messageId: String, order: NewLimitOrder) {

        val limitAsset = if (order.isBuySide()) quotingAsset else baseAsset
        val limitVolume = BigDecimal.valueOf(if (order.isBuySide()) NumberUtils.round(order.getAbsVolume() * order.price, limitAsset.accuracy, true) else order.getAbsVolume())

        val orderInfo = orderInfo(order)
        val availableBalance = availableBalances[limitAsset.assetId]!!

        try {
            validateLimitOrder(order, orderBook, assetPair, availableBalance, limitVolume)
        } catch (e: OrderValidationException) {
            LOGGER.info(e.message)
            order.status = e.orderStatus.name
            addToReportIfNotTrusted(order)
            processedOrders.add(ProcessedOrder(order, false, e.message))
            return
        }

        if (orderBook.leadToNegativeSpread(order)) {
            val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()), messageId, availableBalance.toDouble())
            val orderCopy = matchingResult.order as NewLimitOrder
            val orderStatus = orderCopy.status
            when (OrderStatus.valueOf(orderStatus)) {
                OrderStatus.NoLiquidity -> {
                    addToReportIfNotTrusted(order)
                    processedOrders.add(ProcessedOrder(order, false))
                }
                OrderStatus.ReservedVolumeGreaterThanBalance -> {
                    addToReportIfNotTrusted(order)
                    processedOrders.add(ProcessedOrder(order, false, "Reserved volume is higher than available balance"))
                }
                OrderStatus.NotEnoughFunds -> {
                    addToReportIfNotTrusted(order)
                    processedOrders.add(ProcessedOrder(order, false))
                }
                OrderStatus.InvalidFee -> {
                    addToReportIfNotTrusted(order)
                    processedOrders.add(ProcessedOrder(order, false))
                }
                OrderStatus.Matched,
                OrderStatus.Processing -> {
                    if (processMatchingResult(matchingResult, orderCopy, orderInfo, order, limitAsset)) return
                }
                OrderStatus.InOrderBook -> {
                    if (processMatchingResult(matchingResult, orderCopy, orderInfo, order, limitAsset)) return
                }
                else -> {
                    LOGGER.error("Not handled order status: ${matchingResult.order.status}")
                }
            }
            LOGGER.info("$orderInfo matched")
            return
        }


        order.reservedLimitVolume = limitVolume.toDouble()
        orderBook.addOrder(order)
        ordersToAdd.add(order)
        addToReport(order)
        processedOrders.add(ProcessedOrder(order, true))

        availableBalances[limitAsset.assetId] = availableBalance - limitVolume
        if (!isTrustedClient) {
            walletOperationsProcessor.preProcess(listOf(
                    WalletOperation(UUID.randomUUID().toString(), null, clientId, limitAsset.assetId, date, 0.0, NumberUtils.parseDouble(limitVolume.toDouble(), limitAsset.accuracy).toDouble())
            ), true)
        }

        if (order.isBuySide()) {
            buySideOrderBookChanged = true
        } else {
            sellSideOrderBookChanged = true
        }

        LOGGER.info("$orderInfo added to order book")
    }

    private fun processMatchingResult(matchingResult: MatchingResult, orderCopy: NewLimitOrder, orderInfo: String, order: NewLimitOrder, limitAsset: Asset): Boolean {
        val cancelledOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
        val cancelledTrustedOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
        val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            val result = genericLimitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.map {
                val cancelledOrder = it.copy
                cancelledOrder.status = OrderStatus.Cancelled.name
                cancelledOrder
            })
            cancelledOrdersWalletOperations.addAll(result.walletOperations)
            cancelledOrdersWithTrades.addAll(result.clientLimitOrderWithTrades)
            cancelledTrustedOrdersWithTrades.addAll(result.trustedClientLimitOrderWithTrades)
        }
        val preProcessUncompletedOrderResult = orderServiceHelper.preProcessUncompletedOrder(matchingResult, assetPair, cancelledOrdersWalletOperations)

        val ownWalletOperations = LinkedList<WalletOperation>(matchingResult.ownCashMovements)
        if (assetPair.minVolume != null && orderCopy.getAbsRemainingVolume() < assetPair.minVolume) {
            LOGGER.info("$orderInfo:  Cancelled due to min remaining volume (${NumberUtils.roundForPrint(orderCopy.getAbsRemainingVolume())} < ${NumberUtils.roundForPrint(assetPair.minVolume)})")
            orderCopy.status = OrderStatus.Cancelled.name
        } else {
            orderCopy.reservedLimitVolume = if (order.isBuySide()) NumberUtils.round(orderCopy.getAbsRemainingVolume() * orderCopy.price, limitAsset.accuracy, false) else orderCopy.getAbsRemainingVolume()
            if (!isTrustedClient) {
                val newReservedBalance = NumberUtils.parseDouble(orderCopy.reservedLimitVolume!!, limitAsset.accuracy).toDouble()
                ownWalletOperations.add(WalletOperation(UUID.randomUUID().toString(), null, orderCopy.clientId, limitAsset.assetId, matchingResult.timestamp, 0.0, newReservedBalance))
            }
        }

        try {
            walletOperationsProcessor.preProcess(ownWalletOperations).preProcess(matchingResult.oppositeCashMovements, true)
            try {
                walletOperationsProcessor.preProcess(cancelledOrdersWalletOperations)
            } catch (e: BalanceException) {
                LOGGER.error("$orderInfo: Unable to process cancelled orders wallet operations after matching: ${e.message}")
            }
        } catch (e: BalanceException) {
            val message = "$orderInfo: Unable to process wallet operations after matching: ${e.message}"
            LOGGER.error(message)
            order.status = OrderStatus.NotEnoughFunds.name
            addToReportIfNotTrusted(order)
            processedOrders.add(ProcessedOrder(order, false, message))
            return true
        }

        matchingResult.apply()
        completedOrders.addAll(matchingResult.completedLimitOrders.map { it.origin!! })

        val originCancelledLimitOrders = matchingResult.cancelledLimitOrders.map { it.origin!! }
        if (originCancelledLimitOrders.isNotEmpty()) {
            ordersToCancel.addAll(originCancelledLimitOrders)
            trustedClientsLimitOrdersWithTrades.addAll(cancelledTrustedOrdersWithTrades)
            clientsLimitOrdersWithTrades.addAll(cancelledOrdersWithTrades)
            buySideOrderBookChanged = buySideOrderBookChanged || originCancelledLimitOrders.any { it.isBuySide() }
            sellSideOrderBookChanged = sellSideOrderBookChanged || originCancelledLimitOrders.any { !it.isBuySide() }
        }

        matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }
        orderServiceHelper.processUncompletedOrder(matchingResult, preProcessUncompletedOrderResult, ordersToCancel)

        orderBook.setOrderBook(!order.isBuySide(), matchingResult.orderBook)
        lkkTrades.addAll(matchingResult.lkkTrades)

        clientsLimitOrdersWithTrades.add(LimitOrderWithTrades(order, matchingResult.marketOrderTrades.map { it ->
            LimitTradeInfo(it.tradeId, it.marketClientId, it.marketAsset, it.marketVolume, it.price, it.timestamp, it.limitOrderId, it.limitOrderExternalId, it.limitAsset, it.limitClientId, it.limitVolume, it.index, it.feeInstruction, it.feeTransfer, it.fees, it.absoluteSpread, it.relativeSpread)
        }.toMutableList()))

        matchingResult.limitOrdersReport?.orders?.forEach { orderReport ->
            var orderWithTrades = clientsLimitOrdersWithTrades.find { it.order.id == orderReport.order.id }
            if (orderWithTrades == null) {
                orderWithTrades = LimitOrderWithTrades(orderReport.order)
                clientsLimitOrdersWithTrades.add(orderWithTrades)
            }
            orderWithTrades.trades.addAll(orderReport.trades)
        }

        orderBook.addOrder(order)
        ordersToAdd.add(order)

        availableBalances[limitAsset.assetId] = BigDecimal.valueOf(matchingResult.marketBalance!!)

        buySideOrderBookChanged = true
        sellSideOrderBookChanged = true
        processedOrders.add(ProcessedOrder(order, true))
        return false
    }

    private fun validateLimitOrder(order: NewLimitOrder, orderBook: AssetOrderBook, assetPair: AssetPair, availableBalance: BigDecimal, limitVolume: BigDecimal) {
        if (order.clientId != clientId) {
            throw OrderValidationException(OrderStatus.Cancelled, "${orderInfo(order)} has invalid clientId: ${order.clientId}")
        }
        if (order.assetPairId != assetPair.assetPairId) {
            throw OrderValidationException(OrderStatus.Cancelled, "${orderInfo(order)} has invalid assetPairId: ${order.assetPairId}")
        }
        if (order.status == OrderStatus.NotFoundPrevious.name) {
            throw OrderValidationException(OrderStatus.NotFoundPrevious, "${orderInfo(order)} has not found previous order (${order.previousExternalId})")
        }

        if (!isTrustedClient) {
            validator.validateFee(order)
            validator.validateAssets(assetPair)
            validator.checkBalance(availableBalance, limitVolume)
        }
        validator.validatePrice(order)
        validator.validateVolume(order)
        validator.validatePriceAccuracy(order)
        validator.validateVolumeAccuracy(order)

        if (orderBook.leadToNegativeSpreadForClient(order)) {
            throw OrderValidationException(OrderStatus.LeadToNegativeSpread, "${orderInfo(order)} lead to negative spread")
        }
    }

    private fun orderInfo(order: NewLimitOrder) = "Limit order (id: ${order.externalId})"

    private fun addToReportIfNotTrusted(order: NewLimitOrder) {
        if (!isTrustedClient) {
            clientsLimitOrdersWithTrades.add(LimitOrderWithTrades(order))
        }
    }

    private fun addToReport(order: NewLimitOrder) {
        if (isTrustedClient) {
            trustedClientsLimitOrdersWithTrades.add(LimitOrderWithTrades(order))
        } else {
            clientsLimitOrdersWithTrades.add(LimitOrderWithTrades(order))
        }
    }

}