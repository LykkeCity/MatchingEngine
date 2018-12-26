package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.order.process.common.OrderUtils
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.services.utils.MidPriceUtils
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.utils.NumberUtils
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class StopOrderBookProcessor(private val limitOrderProcessor: LimitOrderProcessor,
                             private val applicationSettingsHolder: ApplicationSettingsHolder,
                             private val executionContextFactory: ExecutionContextFactory,
                             private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder,
                             private val midPriceHolder: MidPriceHolder) {

    fun checkAndExecuteStopLimitOrders(executionContext: ExecutionContext): List<ProcessedOrder> {
        val processedOrders = mutableListOf<ProcessedOrder>()
        var order = getStopOrderToExecute(executionContext)


        while (order != null) {
            val assetPair = executionContext.assetPairsById[order.assetPairId]!!
            val stopOrderExecutionContext = executionContextFactory.create(executionContext)
            val processedOrder = processStopOrder(order, stopOrderExecutionContext)

            val midPriceAfterOrderProcessing = if (stopOrderExecutionContext.orderBooksHolder.isOrderBookChanged()) {
                stopOrderExecutionContext.orderBooksHolder.getOrderBook(assetPair.assetPairId).getMidPrice()
            } else {
                null
            }

            val (lowerMidPriceBound, upperMidPriceBound) = MidPriceUtils.getMidPricesInterval(priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPair.assetPairId, executionContext),
                    midPriceHolder.getReferenceMidPrice(assetPair, executionContext))

            val resultProcessedOrder = if (OrderValidationUtils.isMidPriceValid(midPriceAfterOrderProcessing, lowerMidPriceBound, upperMidPriceBound)) {
                processedOrder
            } else {
                rejectStopOrderHighMidPriceDeviation(order, stopOrderExecutionContext, lowerMidPriceBound, upperMidPriceBound, midPriceAfterOrderProcessing)
            }
            processedOrders.add(resultProcessedOrder)
            order = getStopOrderToExecute(executionContext)
        }
        return processedOrders
    }

    private fun rejectStopOrderHighMidPriceDeviation(order: LimitOrder,
                                        executionContext: ExecutionContext,
                                        lowerMidPriceBound: BigDecimal?,
                                        upperMidPriceBound: BigDecimal?,
                                        midPrice: BigDecimal?): ProcessedOrder {
        val childLimitOrder = OrderUtils.createChildLimitOrder(order, executionContext.date)
        val assetPair = executionContext.assetPairsById[order.assetPairId]!!

        if (!applicationSettingsHolder.isTrustedClient(childLimitOrder.clientId)) {
            executionContext.controlsInfo("Limit order externalId = ${order.externalId}, assetPair = ${assetPair.assetPairId}, mid price control failed, " +
                    "l = ${NumberUtils.roundForPrint(lowerMidPriceBound)}, u = ${NumberUtils.roundForPrint(upperMidPriceBound)}, " +
                    "m = $midPrice")
        }

        return rejectOrder(executionContext, order)
    }

    private fun rejectOrder(executionContext: ExecutionContext,
                            limitOrder: LimitOrder): ProcessedOrder {

        if (!applicationSettingsHolder.isTrustedClient(limitOrder.clientId)) {
            executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(limitOrder))
        }
        return ProcessedOrder(limitOrder, false)
    }


    private fun processStopOrder(order: LimitOrder, executionContext: ExecutionContext): ProcessedOrder {
        reduceReservedBalance(order, executionContext)
        val childLimitOrder = OrderUtils.createChildLimitOrder(order, executionContext.date)
        order.childOrderExternalId = childLimitOrder.externalId
        addOrderToReport(order, executionContext)
        executionContext.info("Created child limit order (${childLimitOrder.externalId}) based on stop order ${order.externalId}")
        return limitOrderProcessor.processOrder(childLimitOrder, executionContext)
    }

    private fun addOrderToReport(order: LimitOrder, executionContext: ExecutionContext) {
        if (applicationSettingsHolder.isTrustedClient(order.clientId)) {
            executionContext.addTrustedClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        } else {
            executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(order))
        }
    }

    private fun getStopOrderToExecute(executionContext: ExecutionContext): LimitOrder? {
        executionContext.assetPairsById.keys.forEach { assetPairId ->
            getStopOrderToExecuteByAssetPair(assetPairId, executionContext)?.let { stopOrderToExecute ->
                return stopOrderToExecute
            }
        }
        return null
    }

    private fun getStopOrderToExecuteByAssetPair(assetPairId: String, executionContext: ExecutionContext): LimitOrder? {
        val orderBook = executionContext.orderBooksHolder.getOrderBook(assetPairId)
        val bestBidPrice = orderBook.getBidPrice()
        val bestAskPrice = orderBook.getAskPrice()
        val order = executionContext.stopOrderBooksHolder.pollStopOrderToExecute(assetPairId,
                bestBidPrice,
                bestAskPrice,
                executionContext.date)
        if (order != null) {
            executionContext.info("Process stop order ${order.externalId}, client ${order.clientId}, asset pair $assetPairId" +
                    " (bestBidPrice=$bestBidPrice, bestAskPrice=$bestAskPrice) due to message ${executionContext.messageId}")
        }
        return order
    }

    private fun reduceReservedBalance(order: LimitOrder, executionContext: ExecutionContext) {
        val reservedVolume = order.reservedLimitVolume!!
        order.reservedLimitVolume = null
        if (applicationSettingsHolder.isTrustedClient(order.clientId)) {
            return
        }
        val assetPair = executionContext.assetPairsById[order.assetPairId]!!
        val limitAssetId = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        val walletOperation = WalletOperation(order.clientId,
                limitAssetId,
                BigDecimal.ZERO,
                -reservedVolume)
        executionContext.walletOperationsProcessor.preProcess(listOf(walletOperation), true)
    }

}