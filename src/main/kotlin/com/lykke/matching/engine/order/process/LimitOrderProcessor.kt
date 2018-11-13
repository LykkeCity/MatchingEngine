package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MidPrice
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.process.context.LimitOrderExecutionContext
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.LimitTradeInfo
import com.lykke.matching.engine.outgoing.messages.v2.enums.TradeRole
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class LimitOrderProcessor(private val limitOrderInputValidator: LimitOrderInputValidator,
                          private val limitOrderBusinessValidator: LimitOrderBusinessValidator,
                          private val applicationSettingsCache: ApplicationSettingsCache,
                          private val matchingEngine: MatchingEngine,
                          private val midPriceHolder: MidPriceHolder,
                          private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder,
                          private val matchingResultHandlingHelper: MatchingResultHandlingHelper) : OrderProcessor<LimitOrder> {
    override fun processOrder(order: LimitOrder, executionContext: ExecutionContext): ProcessedOrder {


        val orderContext = LimitOrderExecutionContext(order, executionContext)
        orderContext.availableLimitAssetBalance = calculateAvailableBalance(orderContext)
        val validationResult = validateOrder(orderContext)
        return if (validationResult.isValid) {
            processValidOrder(orderContext)
        } else {
            orderContext.validationResult = validationResult
            processInvalidOrder(orderContext)
        }
    }

    private fun validateOrder(orderContext: LimitOrderExecutionContext): OrderValidationResult {
        val preProcessorValidationResult = orderContext.executionContext.preProcessorValidationResultsByOrderId[orderContext.order.id]
        if (preProcessorValidationResult != null && !preProcessorValidationResult.isValid) {
            return preProcessorValidationResult
        }
        // fixme: input validator will be moved from the business thread after multilimit order context release
        val inputValidationResult = performInputValidation(orderContext)
        return if (!inputValidationResult.isValid) inputValidationResult else performBusinessValidation(orderContext)
    }

    private fun performInputValidation(orderContext: LimitOrderExecutionContext): OrderValidationResult {
        val order = orderContext.order
        val assetPair = orderContext.executionContext.assetPairsById[order.assetPairId]
        val baseAsset = assetPair?.let { orderContext.executionContext.assetsById[assetPair.baseAssetId] }
        try {
            limitOrderInputValidator.validateLimitOrder(applicationSettingsCache.isTrustedClient(order.clientId),
                    order,
                    assetPair,
                    order.assetPairId,
                    baseAsset)
        } catch (e: OrderValidationException) {
            return OrderValidationResult(false, false, e.message, e.orderStatus)
        }
        return OrderValidationResult(true)
    }

    private fun performBusinessValidation(orderContext: LimitOrderExecutionContext): OrderValidationResult {
        val order = orderContext.order
        try {
            limitOrderBusinessValidator.performValidation(applicationSettingsCache.isTrustedClient(order.clientId),
                    order,
                    orderContext.availableLimitAssetBalance!!,
                    orderContext.limitVolume!!,
                    orderContext.executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(order.assetPairId),
                    orderContext.executionContext.date)
        } catch (e: OrderValidationException) {
            return OrderValidationResult(false, false, e.message, e.orderStatus)
        }
        return OrderValidationResult(true)
    }

    private fun processInvalidOrder(orderContext: LimitOrderExecutionContext): ProcessedOrder {
        val order = orderContext.order
        val validationResult = orderContext.validationResult!!
        orderContext.executionContext.error("${getOrderInfo(order)} is rejected: ${validationResult.message}")
        rejectOrder(orderContext, validationResult.status!!)
        return ProcessedOrder(order, false, validationResult.message)
    }

    private fun rejectOrder(orderContext: LimitOrderExecutionContext, status: OrderStatus) {
        orderContext.order.updateStatus(status, orderContext.executionContext.date)
        addOrderToReportIfNotTrusted(orderContext.order, orderContext.executionContext)
    }

    private fun addOrderToReportIfNotTrusted(order: LimitOrder, executionContext: ExecutionContext) {
        if (!matchingResultHandlingHelper.isOrderForTrustedClientsReport(order)) {
            executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        }
    }

    private fun addOrderToReport(order: LimitOrder, executionContext: ExecutionContext) {
        if (matchingResultHandlingHelper.isOrderForTrustedClientsReport(order)) {
            executionContext.addTrustedClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        } else {
            executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        }
    }

    private fun processValidOrder(orderContext: LimitOrderExecutionContext): ProcessedOrder {
        val (lowerMidPriceBound, upperMidPriceBound) = getMidPricesInterval(orderContext)
        orderContext.lowerMidPriceBound = lowerMidPriceBound
        orderContext.upperMidPriceBound = upperMidPriceBound

        val order = orderContext.order
        val orderBook = orderContext.executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(order.assetPairId)
        if (orderBook.leadToNegativeSpread(order)) {
            return matchOrder(orderContext)
        }

        if (order.timeInForce == OrderTimeInForce.IOC || order.timeInForce == OrderTimeInForce.FOK) {
            order.updateStatus(OrderStatus.Cancelled, orderContext.executionContext.date)
            orderContext.executionContext.info("${getOrderInfo(order)} cancelled due to IOC")
            addOrderToReport(order, orderContext.executionContext)
            return ProcessedOrder(order, true)
        }

        if (!checkMidPriceWithoutMatching(orderContext)) {
            order.updateStatus(OrderStatus.TooHighMidPriceDeviation, orderContext.executionContext.date)
            addOrderToReportIfNotTrusted(order, orderContext.executionContext)
            return ProcessedOrder(order, false)
        }

        return addOrderToOrderBook(orderContext)
    }

    private fun matchOrder(orderContext: LimitOrderExecutionContext): ProcessedOrder {
        val executionContext = orderContext.executionContext
        val order = orderContext.order
        val orderBook = executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(order.assetPairId)

        if (!OrderValidationUtils.isMidPriceValid(orderBook.getMidPrice(), orderContext.lowerMidPriceBound, orderContext.upperMidPriceBound)) {
            executionContext.controlsError("${getOrderInfo(order)} assetPairId = ${order.assetPairId}, is rejected because order book mid price: ${orderBook.getMidPrice()} " +
                    "already aut of range lowerBound: ${orderContext.lowerMidPriceBound}, upperBound: ${orderContext.upperMidPriceBound}")
            rejectOrder(orderContext, OrderStatus.TooHighMidPriceDeviation)
            return ProcessedOrder(order, false)
        }

        val matchingResult = matchingEngine.match(order,
                orderBook.getOrderBook(!order.isBuySide()),
                executionContext.messageId,
                orderContext.availableLimitAssetBalance!!,
                lowerMidPriceBound = orderContext.lowerMidPriceBound,
                upperMidPriceBound = orderContext.upperMidPriceBound,
                executionContext = executionContext)
        orderContext.matchingResult = matchingResult
        val orderCopy = matchingResult.orderCopy as LimitOrder
        val orderStatus = orderCopy.status

        when (OrderStatus.valueOf(orderStatus)) {
            OrderStatus.NoLiquidity,
            OrderStatus.ReservedVolumeGreaterThanBalance,
            OrderStatus.NotEnoughFunds,
            OrderStatus.InvalidFee,
            OrderStatus.TooHighMidPriceDeviation -> {
                return processRejectedMatchingResult(orderContext)
            }
            OrderStatus.InOrderBook,
            OrderStatus.Matched,
            OrderStatus.Processing -> {
                val processedOrder = processMatchingResult(orderContext)
                if (processedOrder.accepted) {
                    executionContext.info("${getOrderInfo(order)} matched")
                }
                return processedOrder
            }
            else -> {
                executionContext.error("Not handled order status: $orderStatus")
                return ProcessedOrder(order, false)
            }
        }
    }

    private fun processRejectedMatchingResult(orderContext: LimitOrderExecutionContext): ProcessedOrder {
        val matchingResult = orderContext.matchingResult!!
        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            matchingResultHandlingHelper.preProcessCancelledOppositeOrders(orderContext)
            matchingResultHandlingHelper.preProcessCancelledOrdersWalletOperations(orderContext)
            matchingResultHandlingHelper.processCancelledOppositeOrders(orderContext)
            val orderBook = orderContext.executionContext.orderBooksHolder
                    .getChangedOrderBookCopy(orderContext.order.assetPairId)
            orderContext.matchingResult!!.cancelledLimitOrders.forEach {
                orderBook.removeOrder(it.origin!!)
            }
        }
        addOrderToReportIfNotTrusted(orderContext.order, orderContext.executionContext)
        return ProcessedOrder(orderContext.order, false)
    }

    private fun processMatchingResult(orderContext: LimitOrderExecutionContext): ProcessedOrder {
        val matchingResult = orderContext.matchingResult!!

        matchingResultHandlingHelper.formOppositeOrderBookAfterMatching(orderContext)

        val order = orderContext.order
        if (!checkMidPriceAfterMatching(orderContext)) {
            orderContext.order.updateStatus(OrderStatus.TooHighMidPriceDeviation, orderContext.executionContext.date)
            return processRejectedMatchingResult(orderContext)
        }

        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            matchingResultHandlingHelper.preProcessCancelledOppositeOrders(orderContext)
        }

        if (matchingResult.uncompletedLimitOrderCopy != null) {
            matchingResultHandlingHelper.preProcessUncompletedOppositeOrder(orderContext)
        }

        orderContext.ownWalletOperations = matchingResult.ownCashMovements
        val orderCopy = matchingResult.orderCopy as LimitOrder
        if (isNotCompletedOrder(orderCopy)) {
            val processedOrder = preProcessPartiallyMatchedIncomingOrder(orderContext)
            if (processedOrder != null) {
                return processedOrder
            }
        }

        try {
            matchingResultHandlingHelper.processWalletOperations(orderContext)
        } catch (e: BalanceException) {
            val message = "${getOrderInfo(orderCopy)}: Unable to process wallet operations after matching: ${e.message}"
            orderContext.executionContext.removeMidPrice(order.assetPairId)
            orderContext.executionContext.error(message)
            order.updateStatus(OrderStatus.NotEnoughFunds, orderContext.executionContext.date)
            addOrderToReportIfNotTrusted(orderContext.order, orderContext.executionContext)
            return ProcessedOrder(order, false, message)
        }

        matchingResult.apply()
        processOppositeOrders(orderContext)
        addMatchedResultToEventData(orderContext)

        if (isNotCompletedOrder(orderCopy)) {
            orderContext.executionContext.orderBooksHolder.addOrder(order)
        }

        return ProcessedOrder(order, true)
    }

    private fun isNotCompletedOrder(order: LimitOrder): Boolean {
        return order.status == OrderStatus.Processing.name || order.status == OrderStatus.InOrderBook.name
    }

    private fun checkMidPriceWithoutMatching(orderContext: LimitOrderExecutionContext): Boolean {
        val order = orderContext.order
        val orderSideBestPrice = getOrderSideBestPriceWithoutMatching(orderContext)
        val oppositeSideBestPrice = orderContext.executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(order.assetPairId).getBestPrice(!order.isBuySide())
        return checkMidPrice(orderSideBestPrice, oppositeSideBestPrice, orderContext)
    }

    private fun checkMidPriceAfterMatching(orderContext: LimitOrderExecutionContext): Boolean {
        val orderSideBestPrice = getOrderSideBestPriceAfterMatching(orderContext)
        val oppositeSideBestPrice = orderContext.matchingResult!!.orderBook.peek()?.price ?: BigDecimal.ZERO
        return checkMidPrice(orderSideBestPrice, oppositeSideBestPrice, orderContext)
    }

    fun checkMidPrice(orderSideBestPrice: BigDecimal, oppositeSideBestPrice: BigDecimal, orderContext: LimitOrderExecutionContext): Boolean {
        val midPrice = getMidPrice(orderSideBestPrice, oppositeSideBestPrice)

        if (OrderValidationUtils.isMidPriceValid(midPrice, orderContext.lowerMidPriceBound, orderContext.upperMidPriceBound)) {
            midPrice?.let{
                orderContext.executionContext.updateMidPrice(MidPrice(orderContext.order.assetPairId, midPrice, orderContext.executionContext.date.time))
            }
            orderContext.executionContext.controlsInfo("${getOrderInfo(orderContext.order)}, assetPair = ${orderContext.order.assetPairId} mid price control passed, " +
                    "lowerMidPriceBound = ${orderContext.lowerMidPriceBound}, upperMidPriceBound = ${orderContext.upperMidPriceBound}, " +
                    "midPrice = $midPrice")
            return true
        }

        orderContext.executionContext.controlsError("${getOrderInfo(orderContext.order)}, assetPair = ${orderContext.order} is rejected: too high mid price deviation, " +
                "lowerMidPriceBound = ${orderContext.lowerMidPriceBound}, upperMidPriceBound = ${orderContext.upperMidPriceBound}, " +
                "midPrice = $midPrice")
        return false
    }

    private fun getOrderSideBestPriceWithoutMatching(orderContext: LimitOrderExecutionContext): BigDecimal {
        val order = orderContext.order
        val orderBook = orderContext.executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(order.assetPairId)
        return getOrderSideBestPrice(orderContext.order, orderBook, orderContext)
    }

    private fun getOrderSideBestPriceAfterMatching(orderContext: LimitOrderExecutionContext): BigDecimal {
        val orderCopy = orderContext.matchingResult!!.orderCopy as LimitOrder
        val orderBook = orderContext.executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(orderCopy.assetPairId)
        return getOrderSideBestPrice(orderCopy, orderBook, orderContext)
    }

    fun getOrderSideBestPrice(order: LimitOrder, assetOrderBook: AssetOrderBook, orderContext: LimitOrderExecutionContext): BigDecimal {
        val bestPrice = assetOrderBook.getBestPrice(order.isBuySide())

        if (order.status == OrderStatus.Processing.name || order.status == OrderStatus.InOrderBook.name) {
            if (isPartiallyMatchedOrderCancelled(orderContext)) {
                return bestPrice
            }

            if (NumberUtils.equalsIgnoreScale(BigDecimal.ZERO, bestPrice)) {
                return order.price
            }
            if (order.isBuySide()) {
                return if (order.price > bestPrice) order.price else bestPrice
            }

            return if (order.price < bestPrice) order.price else bestPrice
        }

        return bestPrice
    }

    private fun preProcessPartiallyMatchedIncomingOrder(orderContext: LimitOrderExecutionContext): ProcessedOrder? {
        val matchingResult = orderContext.matchingResult!!
        val orderCopy = matchingResult.orderCopy as LimitOrder
        val assetPair = orderContext.executionContext.assetPairsById[orderCopy.assetPairId]!!
        when {
            assetPair.minVolume != null && orderCopy.getAbsRemainingVolume() < assetPair.minVolume -> {
                orderContext.executionContext.info("${getOrderInfo(orderCopy)}: cancelled due to min remaining volume (${NumberUtils.roundForPrint(orderCopy.getAbsRemainingVolume())} < ${NumberUtils.roundForPrint(assetPair.minVolume)})")
                orderCopy.updateStatus(OrderStatus.Cancelled, orderContext.executionContext.date)
            }
            matchingResult.matchedWithZeroLatestTrade -> {
                orderContext.executionContext.info("${getOrderInfo(orderCopy)}: cancelled due to zero latest trade")
                orderCopy.updateStatus(OrderStatus.Cancelled, orderContext.executionContext.date)
            }
            orderCopy.timeInForce == OrderTimeInForce.IOC -> {
                orderContext.executionContext.info("${getOrderInfo(orderCopy)}: cancelled after matching due to IOC, remainingVolume: ${orderCopy.remainingVolume}")
                orderCopy.updateStatus(OrderStatus.Cancelled, orderContext.executionContext.date)
            }
            orderCopy.timeInForce == OrderTimeInForce.FOK -> {
                orderContext.executionContext.info("${getOrderInfo(orderCopy)}: cancelled after matching due to FOK, remainingVolume: ${orderCopy.remainingVolume}")
                orderContext.order.updateStatus(OrderStatus.Cancelled, orderContext.executionContext.date)
                addOrderToReport(orderContext.order, orderContext.executionContext)
                return ProcessedOrder(orderContext.order, true)
            }
            else -> {
                val limitAsset = orderContext.limitAsset!!
                orderCopy.reservedLimitVolume = if (orderCopy.isBuySide()) NumberUtils.setScaleRoundDown(orderCopy.getAbsRemainingVolume() * orderCopy.price, limitAsset.accuracy) else orderCopy.getAbsRemainingVolume()
                if (!applicationSettingsCache.isTrustedClient(orderCopy.clientId)) {
                    val newReservedBalance = NumberUtils.setScaleRoundHalfUp(orderCopy.reservedLimitVolume!!, limitAsset.accuracy)
                    orderContext.ownWalletOperations!!.add(WalletOperation(orderCopy.clientId,
                            limitAsset.assetId,
                            BigDecimal.ZERO,
                            newReservedBalance))
                }
            }
        }
        return null
    }


    private fun processOppositeOrders(orderContext: LimitOrderExecutionContext) {
        val matchingResult = orderContext.matchingResult!!
        orderContext.executionContext.orderBooksHolder.addCompletedOrders(matchingResult.completedLimitOrders.map { it.origin!! })

        if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
            matchingResultHandlingHelper.processCancelledOppositeOrders(orderContext)
        }
        if (matchingResult.uncompletedLimitOrderCopy != null) {
            matchingResultHandlingHelper.processUncompletedOppositeOrder(orderContext)
        }

        updateCurrentTransactionOrderBookAfterMatching(orderContext)
    }

    private fun updateCurrentTransactionOrderBookAfterMatching(orderContext: LimitOrderExecutionContext) {
        val matchingResult = orderContext.matchingResult!!

        val orderCopy = matchingResult.orderCopy as LimitOrder
        orderContext.executionContext.orderBooksHolder
                .getChangedOrderBookCopy(orderCopy.assetPairId)
                .setOrderBook(!orderCopy.isBuySide(), matchingResult.orderBook)
    }


    private fun addMatchedResultToEventData(orderContext: LimitOrderExecutionContext) {
        val matchingResult = orderContext.matchingResult!!
        orderContext.executionContext.lkkTrades.addAll(matchingResult.lkkTrades)

        val orderCopy = matchingResult.orderCopy as LimitOrder
        val limitOrderWithTrades = LimitOrderWithTrades(orderCopy,
                matchingResult.marketOrderTrades.asSequence().map { it ->
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
        orderContext.executionContext.addClientLimitOrderWithTrades(limitOrderWithTrades)
        matchingResult.limitOrdersReport?.orders?.let { orderContext.executionContext.addClientsLimitOrdersWithTrades(it) }
    }

    private fun addOrderToOrderBook(orderContext: LimitOrderExecutionContext): ProcessedOrder {
        val order = orderContext.order
        val limitVolume = orderContext.limitVolume!!

        val walletOperation = WalletOperation(order.clientId,
                orderContext.limitAsset!!.assetId,
                BigDecimal.ZERO,
                limitVolume)

        try {
            orderContext.executionContext.walletOperationsProcessor.preProcess(listOf(walletOperation))
        } catch (e: BalanceException) {
            orderContext.executionContext.removeMidPrice(order.assetPairId)
            val errorMessage = "Wallet operation leads to invalid balance (${e.message})"
            orderContext.executionContext.error("${getOrderInfo(order)}: $errorMessage")
            rejectOrder(orderContext, OrderStatus.NotEnoughFunds)
            return ProcessedOrder(order, false, errorMessage)
        }

        order.reservedLimitVolume = limitVolume
        orderContext.executionContext.orderBooksHolder.addOrder(order)
        addOrderToReport(orderContext.order.copy(), orderContext.executionContext)

        if (!applicationSettingsCache.isTrustedClient(order.clientId)) {
            orderContext.executionContext.info("${getOrderInfo(order)} added to order book")
        }
        return ProcessedOrder(order, true)
    }

    private fun calculateAvailableBalance(orderContext: LimitOrderExecutionContext): BigDecimal? {
        val balancesGetter = orderContext.executionContext.walletOperationsProcessor
        val clientId = orderContext.order.clientId
        val limitAsset = orderContext.limitAsset ?: return null
        return NumberUtils.setScaleRoundHalfUp(balancesGetter.getAvailableBalance(clientId, limitAsset.assetId), limitAsset.accuracy)
    }

    private fun getOrderInfo(order: LimitOrder) = "Limit order (id: ${order.externalId})"

    private fun getMidPrice(orderSideBestPrice: BigDecimal, oppositeBestPrice: BigDecimal): BigDecimal? {
        if (NumberUtils.equalsIgnoreScale(orderSideBestPrice, BigDecimal.ZERO) || NumberUtils.equalsIgnoreScale(oppositeBestPrice, BigDecimal.ZERO)) {
            return null
        }

        return NumberUtils.divideWithMaxScale((orderSideBestPrice + oppositeBestPrice), BigDecimal.valueOf(2))
    }

    private fun isPartiallyMatchedOrderCancelled(orderContext: LimitOrderExecutionContext): Boolean {
        if (orderContext.matchingResult == null) {
            return false
        }
        val matchingResult = orderContext.matchingResult!!
        val orderCopy = matchingResult.orderCopy as LimitOrder
        val assetPair = orderContext.executionContext.assetPairsById[orderCopy.assetPairId]!!

        return (assetPair.minVolume != null && orderCopy.getAbsRemainingVolume() < assetPair.minVolume) ||
                matchingResult.matchedWithZeroLatestTrade ||
                orderCopy.timeInForce == OrderTimeInForce.FOK ||
                orderCopy.timeInForce == OrderTimeInForce.IOC
    }

    private fun getMidPricesInterval(orderContext: LimitOrderExecutionContext): Pair<BigDecimal?, BigDecimal?> {
        var lowerMidPriceBound: BigDecimal? = null
        var upperMidPriceBound: BigDecimal? = null

        val order = orderContext.order
        val executionContext = orderContext.executionContext

        val midPriceDeviationThreshold = priceDeviationThresholdHolder.getMidPriceDeviationThreshold(order.assetPairId, executionContext)
        val referenceMidPrice = midPriceHolder.getReferenceMidPrice(executionContext.assetPairsById[order.assetPairId]!!, orderContext.executionContext)

        if (midPriceDeviationThreshold != null && referenceMidPrice != null && !NumberUtils.equalsIgnoreScale(referenceMidPrice, BigDecimal.ZERO)) {
            lowerMidPriceBound = referenceMidPrice - (referenceMidPrice * midPriceDeviationThreshold)
            upperMidPriceBound = referenceMidPrice + (referenceMidPrice * midPriceDeviationThreshold)
        }

        return lowerMidPriceBound to upperMidPriceBound
    }


}