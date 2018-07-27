
package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.matching.MatchingResult
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.LimitOrderValidator
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderValidationException
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.enums.TradeRole
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.MessageSender
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
                           private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
                           private val messageSender: MessageSender,
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
    private val ordersToAdd = mutableListOf<LimitOrder>()

    private val lkkTrades = mutableListOf<LkkTrade>()
    private val clientsLimitOrdersWithTrades = clientsLimitOrdersWithTrades.toMutableList()
    private val trustedClientsLimitOrdersWithTrades = trustedClientsLimitOrdersWithTrades.toMutableList()

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
            trustedClientsLimitOrdersQueue.put(LimitOrdersReport(messageId, trustedClientsLimitOrdersWithTrades))
            messageSender.sendTrustedClientsMessage(EventFactory.createTrustedClientsExecutionEvent(trustedClientsSequenceNumber!!,
                    messageId,
                    operationId,
                    date,
                    messageType,
                    trustedClientsLimitOrdersWithTrades))
        }

        if (clientsLimitOrdersWithTrades.isNotEmpty()) {
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

        try {
            validateLimitOrder(order, orderBook, assetPair, availableBalance, limitVolume)
        } catch (e: OrderValidationException) {
            LOGGER.info("Limit order (id: ${order.externalId}) is rejected: ${e.message}")
            order.updateStatus(e.orderStatus, date)
            addToReportIfNotTrusted(order)
            processedOrders.add(ProcessedOrder(order, false, e.message))
            return
        }

        if (orderBook.leadToNegativeSpread(order)) {
            val matchingResult = matchingEngine.match(order, orderBook.getOrderBook(!order.isBuySide()), messageId, availableBalance)
            val orderCopy = matchingResult.order as LimitOrder
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

        LOGGER.info("$orderInfo added to order book")
    }

    private fun processMatchingResult(matchingResult: MatchingResult, orderCopy: LimitOrder, orderInfo: String, order: LimitOrder, limitAsset: Asset): Boolean {
        val cancelledOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
        val cancelledTrustedOrdersWithTrades = LinkedList<LimitOrderWithTrades>()
        val cancelledOrdersWalletOperations = LinkedList<WalletOperation>()
        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            val result = genericLimitOrderService.calculateWalletOperationsForCancelledOrders(matchingResult.cancelledLimitOrders.map {
                val cancelledOrder = it.copy
                cancelledOrder.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
                cancelledOrder
            })
            cancelledOrdersWalletOperations.addAll(result.walletOperations)
            cancelledOrdersWithTrades.addAll(result.clientLimitOrderWithTrades)
            cancelledTrustedOrdersWithTrades.addAll(result.trustedClientLimitOrderWithTrades)
        }
        val preProcessUncompletedOrderResult = orderServiceHelper.preProcessUncompletedOrder(matchingResult, assetPair, cancelledOrdersWalletOperations)

        val ownWalletOperations = LinkedList<WalletOperation>(matchingResult.ownCashMovements)
        if (OrderStatus.Processing.name == orderCopy.status || OrderStatus.InOrderBook.name == orderCopy.status) {
            if (assetPair.minVolume != null && orderCopy.getAbsRemainingVolume() < assetPair.minVolume) {
                LOGGER.info("$orderInfo: Cancelled due to min remaining volume (${NumberUtils.roundForPrint(orderCopy.getAbsRemainingVolume())} < ${NumberUtils.roundForPrint(assetPair.minVolume)})")
                orderCopy.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
            } else if (matchingResult.matchedWithZeroLatestTrade == true) {
                LOGGER.info("$orderInfo: Cancelled due to zero latest trade")
                orderCopy.updateStatus(OrderStatus.Cancelled, matchingResult.timestamp)
            } else {
                orderCopy.reservedLimitVolume = if (order.isBuySide()) NumberUtils.setScaleRoundDown(orderCopy.getAbsRemainingVolume() * orderCopy.price, limitAsset.accuracy) else orderCopy.getAbsRemainingVolume()
                if (!isTrustedClient) {
                    val newReservedBalance = NumberUtils.setScaleRoundHalfUp(orderCopy.reservedLimitVolume!!, limitAsset.accuracy)
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
            order.updateStatus(OrderStatus.NotEnoughFunds, matchingResult.timestamp)
            addToReportIfNotTrusted(order)
            processedOrders.add(ProcessedOrder(order, false, message))
            return false
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

        clientsLimitOrdersWithTrades.add(LimitOrderWithTrades(order.copy(),
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
                            TradeRole.TAKER)
                }.toMutableList()))

        matchingResult.limitOrdersReport?.orders?.forEach { orderReport ->
            var orderWithTrades = clientsLimitOrdersWithTrades.find { it.order.id == orderReport.order.id }
            if (orderWithTrades == null) {
                orderWithTrades = LimitOrderWithTrades(orderReport.order)
                clientsLimitOrdersWithTrades.add(orderWithTrades)
            }
            orderWithTrades.trades.addAll(orderReport.trades)
        }

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

    private fun validateLimitOrder(order: LimitOrder, orderBook: AssetOrderBook, assetPair: AssetPair, availableBalance: BigDecimal, limitVolume: BigDecimal) {
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
        validator.validateVolume(order, assetPair)
        validator.validatePriceAccuracy(order)
        validator.validateVolumeAccuracy(order)

        if (orderBook.leadToNegativeSpreadForClient(order)) {
            throw OrderValidationException(OrderStatus.LeadToNegativeSpread, "${orderInfo(order)} lead to negative spread")
        }
    }

    private fun orderInfo(order: LimitOrder) = "Limit order (id: ${order.externalId})"

    private fun addToReportIfNotTrusted(order: LimitOrder) {
        if (!isTrustedClient) {
            clientsLimitOrdersWithTrades.add(LimitOrderWithTrades(order))
        }
    }

    private fun addToReport(order: LimitOrder) {
        if (isTrustedClient) {
            trustedClientsLimitOrdersWithTrades.add(LimitOrderWithTrades(order))
        } else {
            clientsLimitOrdersWithTrades.add(LimitOrderWithTrades(order))
        }
    }

}