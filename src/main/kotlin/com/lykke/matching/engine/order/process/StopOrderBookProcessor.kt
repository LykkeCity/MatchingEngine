package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.order.transaction.ExecutionContext
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.UUID

@Component
class StopOrderBookProcessor(private val limitOrderProcessor: LimitOrderProcessor,
                             private val applicationSettingsCache: ApplicationSettingsCache) {

    fun checkAndExecuteStopLimitOrders(executionContext: ExecutionContext): List<ProcessedOrder> {
        val processedOrders = mutableListOf<ProcessedOrder>()
        var order = getStopOrderToExecute(executionContext)
        while (order != null) {
            reduceReservedBalance(order, executionContext)
            val processedOrder = limitOrderProcessor.processOrder(order, executionContext)
            processedOrders.add(processedOrder)
            order = getStopOrderToExecute(executionContext)
        }
        return processedOrders
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
        val orderBook = executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(assetPairId)
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
        if (applicationSettingsCache.isTrustedClient(order.clientId)) {
            return
        }
        val assetPair = executionContext.assetPairsById[order.assetPairId]!!
        val limitAssetId = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        val walletOperation = WalletOperation(UUID.randomUUID().toString(),
                null,
                order.clientId,
                limitAssetId,
                executionContext.date,
                BigDecimal.ZERO,
                -reservedVolume)
        executionContext.walletOperationsProcessor.preProcess(listOf(walletOperation), true)
    }

}