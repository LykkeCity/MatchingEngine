package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.process.context.StopLimitOrderContext
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import com.lykke.matching.engine.services.validators.impl.OrderFatalValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.UUID

@Component
class StopLimitOrderProcessor(private val limitOrderInputValidator: LimitOrderInputValidator,
                              private val stopOrderBusinessValidator: StopOrderBusinessValidator,
                              private val applicationSettingsCache: ApplicationSettingsCache,
                              private val limitOrderProcessor: LimitOrderProcessor) : OrderProcessor<LimitOrder> {

    override fun processOrder(order: LimitOrder, executionContext: ExecutionContext): ProcessedOrder {
        val orderContext = StopLimitOrderContext(order, executionContext)
        val validationResult = validateOrder(orderContext)
        return if (validationResult.isValid) {
            processValidOrder(orderContext)
        } else {
            orderContext.validationResult = validationResult
            processInvalidOrder(orderContext)
        }
    }

    private fun validateOrder(orderContext: StopLimitOrderContext): OrderValidationResult {
        val preProcessorValidationResult = orderContext.executionContext.preProcessorValidationResultsByOrderId[orderContext.order.id]
        if (preProcessorValidationResult != null && !preProcessorValidationResult.isValid) {
            return preProcessorValidationResult
        }
        // fixme: input validator will be moved from the business thread after multilimit order context release
        val inputValidationResult = performInputValidation(orderContext)
        return if (!inputValidationResult.isValid) inputValidationResult else performBusinessValidation(orderContext)
    }

    private fun performInputValidation(orderContext: StopLimitOrderContext): OrderValidationResult {
        try {
            limitOrderInputValidator.validateStopOrder(orderContext)
        } catch (e: OrderValidationException) {
            return OrderValidationResult(false, false, e.message, e.orderStatus)
        } catch (e: OrderFatalValidationException) {
            return OrderValidationResult(false, true, e.message)
        }
        return OrderValidationResult(true)
    }

    private fun performBusinessValidation(orderContext: StopLimitOrderContext): OrderValidationResult {
        if (orderContext.limitVolume != null) {
            try {
                stopOrderBusinessValidator.performValidation(calculateAvailableBalance(orderContext), orderContext.limitVolume)
            } catch (e: OrderValidationException) {
                return OrderValidationResult(false, false, e.message, e.orderStatus)
            }
        }
        return OrderValidationResult(true)
    }

    private fun processInvalidOrder(orderContext: StopLimitOrderContext): ProcessedOrder {
        val order = orderContext.order
        val validationResult = orderContext.validationResult!!
        orderContext.executionContext.info("Rejected order: ${getOrderInfo(order)}, rejection reason: ${validationResult.message}")
        rejectOrder(orderContext, validationResult.status!!)
        return ProcessedOrder(order, false, validationResult.message)
    }

    private fun rejectOrder(orderContext: StopLimitOrderContext, status: OrderStatus) {
        orderContext.order.updateStatus(status, orderContext.executionContext.date)
        addOrderToReportIfNotTrusted(orderContext)
    }

    private fun addOrderToReportIfNotTrusted(orderContext: StopLimitOrderContext) {
        val order = orderContext.order
        if (order.isPartiallyMatched() || !applicationSettingsCache.isTrustedClient(order.clientId)) {
            orderContext.executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        }
    }

    private fun addOrderToReport(orderContext: StopLimitOrderContext) {
        val order = orderContext.order
        if (applicationSettingsCache.isTrustedClient(order.clientId) && !order.isPartiallyMatched()) {
            orderContext.executionContext.addTrustedClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        } else {
            orderContext.executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        }
    }

    private fun processValidOrder(orderContext: StopLimitOrderContext): ProcessedOrder {
        return if (isOrderReadyToImmediateExecution(orderContext)) {
            executeOrderImmediately(orderContext)
        } else {
            addOrderToStopOrderBook(orderContext)
        }
    }

    private fun isOrderReadyToImmediateExecution(orderContext: StopLimitOrderContext): Boolean {
        val order = orderContext.order
        val executionContext = orderContext.executionContext
        val orderBook = executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(order.assetPairId)
        val bestBidPrice = orderBook.getBidPrice()
        val bestAskPrice = orderBook.getAskPrice()

        val price: BigDecimal = if (order.lowerLimitPrice != null && (order.isBuySide() && bestAskPrice > BigDecimal.ZERO && bestAskPrice <= order.lowerLimitPrice ||
                        !order.isBuySide() && bestBidPrice > BigDecimal.ZERO && bestBidPrice <= order.lowerLimitPrice)) {
            order.lowerPrice!!
        } else if (order.upperLimitPrice != null && (order.isBuySide() && bestAskPrice >= order.upperLimitPrice ||
                        !order.isBuySide() && bestBidPrice >= order.upperLimitPrice)) {
            order.upperPrice!!
        } else {
            return false
        }
        orderContext.immediateExecutionPrice = price
        executionContext.info("${getOrderInfo(order)} is ready to immediate execution (bestBidPrice=$bestBidPrice, bestAskPrice=$bestAskPrice)")
        return true
    }

    private fun executeOrderImmediately(orderContext: StopLimitOrderContext): ProcessedOrder {
        val order = orderContext.order
        order.updateStatus(OrderStatus.InOrderBook, orderContext.executionContext.date)
        order.price = orderContext.immediateExecutionPrice!!
        return limitOrderProcessor.processOrder(order, orderContext.executionContext)
    }

    private fun addOrderToStopOrderBook(orderContext: StopLimitOrderContext): ProcessedOrder {
        val order = orderContext.order
        val limitVolume = orderContext.limitVolume!!
        val walletOperation = WalletOperation(UUID.randomUUID().toString(),
                null,
                order.clientId,
                orderContext.limitAsset!!.assetId,
                orderContext.executionContext.date,
                BigDecimal.ZERO,
                limitVolume)

        try {
            orderContext.executionContext.walletOperationsProcessor.preProcess(listOf(walletOperation))
        } catch (e: BalanceException) {
            val errorMessage = "Wallet operation leads to invalid balance (${e.message})"
            orderContext.executionContext.error("${getOrderInfo(order)}: $errorMessage")
            rejectOrder(orderContext, OrderStatus.NotEnoughFunds)
            return ProcessedOrder(order, false, errorMessage)
        }
        order.reservedLimitVolume = limitVolume
        orderContext.executionContext.stopOrderBooksHolder.addOrder(order)
        addOrderToReport(orderContext)
        orderContext.executionContext.info("${getOrderInfo(order)} added to stop order book")
        return ProcessedOrder(order, true)
    }

    private fun calculateAvailableBalance(orderContext: StopLimitOrderContext): BigDecimal {
        val balancesGetter = orderContext.executionContext.walletOperationsProcessor
        val clientId = orderContext.order.clientId
        val limitAsset = orderContext.limitAsset!!
        return NumberUtils.setScaleRoundHalfUp(balancesGetter.getAvailableBalance(clientId, limitAsset.assetId), limitAsset.accuracy)
    }

    private fun getOrderInfo(order: LimitOrder): String {
        return "Stop limit order (id: ${order.externalId})"
    }

}
