package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
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
import com.lykke.matching.engine.utils.RoundingUtils
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
                           payBackBaseReserved: BigDecimal,
                           payBackQuotingReserved: BigDecimal,
                           clientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
                           trustedClientsLimitOrdersWithTrades: Collection<LimitOrderWithTrades>,
                           private val LOGGER: Logger) {

    private val validator = LimitOrderValidator(assetsPairsHolder, applicationSettingsCache)

    private val orderServiceHelper = OrderServiceHelper(genericLimitOrderService, LOGGER)
    private val walletOperationsProcessor = balancesHolder.createWalletProcessor(LOGGER, true)

    private val isTrustedClient = balancesHolder.isTrustedClient(clientId)
    private val baseAsset = assetsHolder.getAsset(assetPair.baseAssetId)
    private val quotingAsset = assetsHolder.getAsset(assetPair.quotingAssetId)
    private val availableBalances = HashMap<String, BigDecimal>()

    private var buySideOrderBookChanged = false
    private var sellSideOrderBookChanged = false

    private val ordersToCancel = mutableListOf<NewLimitOrder>()
    private val completedOrders = mutableListOf<NewLimitOrder>()

    private val processedOrders = mutableListOf<ProcessedOrder>()
    private val ordersToAdd = mutableListOf<NewLimitOrder>()

    private val lkkTrades = mutableListOf<LkkTrade>()
    private val clientsLimitOrdersWithTrades = clientsLimitOrdersWithTrades.toMutableList()
    private val trustedClientsLimitOrdersWithTrades = trustedClientsLimitOrdersWithTrades.toMutableList()

    init {
        if (orderBook.assetId != assetPair.assetPairId) {
            throw IllegalArgumentException("Invalid order book asset pair: ${orderBook.assetId}")
        }

        availableBalances[assetPair.baseAssetId] = RoundingUtils.setScaleRoundHalfUp(balancesHolder.getAvailableBalance(clientId, assetPair.baseAssetId, payBackBaseReserved), baseAsset.accuracy)
        availableBalances[assetPair.quotingAssetId] = RoundingUtils.setScaleRoundHalfUp(balancesHolder.getAvailableBalance(clientId, assetPair.quotingAssetId, payBackQuotingReserved), quotingAsset.accuracy)
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

    fun preProcess(orders: Collection<NewLimitOrder>): LimitOrdersProcessor {
        orders.forEach { preProcess(it) }
        return this
    }

    fun apply(operationId: String, operationType: String, pBuySideOrderBookChanged: Boolean, pSellSideOrderBookChanged: Boolean): OrderProcessResult {

        buySideOrderBookChanged = buySideOrderBookChanged || pBuySideOrderBookChanged
        sellSideOrderBookChanged = sellSideOrderBookChanged || pSellSideOrderBookChanged

        walletOperationsProcessor.apply(operationId, operationType)

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
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(trustedClientsLimitOrdersWithTrades))
        }

        if (clientsLimitOrdersWithTrades.isNotEmpty()) {
            clientsLimitOrdersQueue.put(LimitOrdersReport(clientsLimitOrdersWithTrades))
        }

        return OrderProcessResult(processedOrders)
    }

    private fun preProcess(order: NewLimitOrder) {

        val limitAsset = if (order.isBuySide()) quotingAsset else baseAsset
        val limitVolume = if (order.isBuySide()) RoundingUtils.setScaleRoundUp(order.getAbsVolume() * order.price, limitAsset.accuracy) else order.getAbsVolume()

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
            val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()), availableBalance.toDouble())
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
                    val cancelledOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                    val cancelledTrustedOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
                    val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
                    if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                        val result = genericLimitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.toList())
                        cancelledOrdersWalletOperations.addAll(result.walletOperations)
                        cancelledOrdersWithTrades.addAll(result.clientLimitOrderWithTrades)
                        cancelledTrustedOrdersWithTrades.addAll(result.trustedClientLimitOrderWithTrades)
                    }
                    val preProcessUncompletedOrderResult = orderServiceHelper.preProcessUncompletedOrder(matchingResult, assetPair, cancelledOrdersWalletOperations)

                    val ownWalletOperations = LinkedList<WalletOperation>(matchingResult.ownCashMovements)
                    if (orderCopy.status == OrderStatus.Processing.name) {
                        if (assetPair.minVolume != null && orderCopy.getAbsRemainingVolume() < assetPair.minVolume) {
                            LOGGER.info("$orderInfo:  Cancelled due to min remaining volume (${RoundingUtils.roundForPrint(orderCopy.getAbsRemainingVolume())} < ${RoundingUtils.roundForPrint(assetPair.minVolume)})")
                            orderCopy.status = OrderStatus.Cancelled.name
                        } else {
                            orderCopy.reservedLimitVolume = if (order.isBuySide()) RoundingUtils.setScaleRoundUp(orderCopy.getAbsRemainingVolume() * orderCopy.price, limitAsset.accuracy) else orderCopy.getAbsRemainingVolume()
                            if (!isTrustedClient) {
                                val newReservedBalance = RoundingUtils.parseDouble(orderCopy.reservedLimitVolume!!, limitAsset.accuracy)
                                ownWalletOperations.add(WalletOperation(UUID.randomUUID().toString(), null, orderCopy.clientId, limitAsset.assetId, matchingResult.timestamp, BigDecimal.ZERO, newReservedBalance))
                            }
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
                        return
                    }

                    matchingResult.apply()
                    completedOrders.addAll(matchingResult.completedLimitOrders)

                    if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                        ordersToCancel.addAll(matchingResult.cancelledLimitOrders)
                        trustedClientsLimitOrdersWithTrades.addAll(cancelledTrustedOrdersWithTrades)
                        clientsLimitOrdersWithTrades.addAll(cancelledOrdersWithTrades)
                        buySideOrderBookChanged = buySideOrderBookChanged || matchingResult.cancelledLimitOrders.any { it.isBuySide() }
                        sellSideOrderBookChanged = sellSideOrderBookChanged || matchingResult.cancelledLimitOrders.any { !it.isBuySide() }
                    }

                    matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }
                    matchingResult.uncompletedLimitOrder?.let { uncompletedLimitOrder ->
                        if (preProcessUncompletedOrderResult) {
                            matchingResult.orderBook.put(uncompletedLimitOrder)
                        } else {
                            ordersToCancel.add(uncompletedLimitOrder)
                        }
                    }

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

                    if (order.status == OrderStatus.Processing.name) {
                        orderBook.addOrder(order)
                        ordersToAdd.add(order)
                    }

                    availableBalances[limitAsset.assetId] = BigDecimal.valueOf(matchingResult.marketBalance!!)

                    buySideOrderBookChanged = true
                    sellSideOrderBookChanged = true
                    processedOrders.add(ProcessedOrder(order, true))
                }
                else -> {
                    LOGGER.error("Not handled order status: ${matchingResult.order.status}")
                }
            }
            LOGGER.info("$orderInfo matched")
            return
        }


        order.reservedLimitVolume = limitVolume
        orderBook.addOrder(order)
        ordersToAdd.add(order)
        addToReport(order)
        processedOrders.add(ProcessedOrder(order, true))

        availableBalances[limitAsset.assetId] = availableBalance - limitVolume
        if (!isTrustedClient) {
            walletOperationsProcessor.preProcess(listOf(
                    WalletOperation(UUID.randomUUID().toString(), null, clientId, limitAsset.assetId, date, BigDecimal.ZERO, RoundingUtils.parseDouble(limitVolume, limitAsset.accuracy))
            ), true)
        }

        if (order.isBuySide()) {
            buySideOrderBookChanged = true
        } else {
            sellSideOrderBookChanged = true
        }

        LOGGER.info("$orderInfo added to order book")
    }

    private fun validateLimitOrder(order: NewLimitOrder, orderBook: AssetOrderBook, assetPair: AssetPair, availableBalance: BigDecimal, limitVolume: BigDecimal) {
        if (order.clientId != clientId) {
            throw OrderValidationException("${orderInfo(order)} has invalid clientId: ${order.clientId}", OrderStatus.Cancelled)
        }
        if (order.assetPairId != assetPair.assetPairId) {
            throw OrderValidationException("${orderInfo(order)} has invalid assetPairId: ${order.assetPairId}", OrderStatus.Cancelled)
        }
        if (order.status == OrderStatus.NotFoundPrevious.name) {
            throw OrderValidationException("${orderInfo(order)} has not found previous order (${order.previousExternalId})", OrderStatus.NotFoundPrevious)
        }

        if (!isTrustedClient) {
            validator.validateFee(order)
            validator.validateAssets(assetPair)
            validator.checkBalance(availableBalance, limitVolume)
        }
        validator.validatePrice(order)
        validator.validateVolume(order)

        if (orderBook.leadToNegativeSpreadForClient(order)) {
            throw OrderValidationException("${orderInfo(order)} lead to negative spread", OrderStatus.LeadToNegativeSpread)
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