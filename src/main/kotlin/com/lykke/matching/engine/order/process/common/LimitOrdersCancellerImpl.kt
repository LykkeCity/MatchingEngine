package com.lykke.matching.engine.order.process.common

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.transaction.AbstractTransactionOrderBooksHolder
import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.plus
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class LimitOrdersCancellerImpl(private val applicationSettingsHolder: ApplicationSettingsHolder) : LimitOrdersCanceller {

    override fun cancelOrders(limitOrdersToCancel: Collection<LimitOrder>,
                              limitOrdersToReplace: Collection<LimitOrder>,
                              stopLimitOrdersToCancel: Collection<LimitOrder>,
                              stopLimitOrdersToReplace: Collection<LimitOrder>,
                              executionContext: ExecutionContext) {
        val allLimitOrders = plus(limitOrdersToCancel, limitOrdersToReplace)
        val allStopLimitOrders = plus(stopLimitOrdersToCancel, stopLimitOrdersToReplace)
        processWalletOperationsForCancelledOrders(allLimitOrders,
                allStopLimitOrders,
                executionContext,
                true)
        removeOrders(limitOrdersToCancel,
                limitOrdersToReplace,
                executionContext.orderBooksHolder)
        removeOrders(stopLimitOrdersToCancel,
                stopLimitOrdersToReplace,
                executionContext.stopOrderBooksHolder)
        addLimitOrdersInfoToExecutionEventData(plus(allLimitOrders, allStopLimitOrders), executionContext)
    }

    private fun removeOrders(cancelledOrders: Collection<LimitOrder>,
                             replacedOrders: Collection<LimitOrder>,
                             orderBooksHolder: AbstractTransactionOrderBooksHolder<*, *>) {
        plus(cancelledOrders, replacedOrders).forEach { order ->
            orderBooksHolder.getChangedOrderBookCopy(order.assetPairId).removeOrder(order)
        }
        orderBooksHolder.removeOrdersFromMapsAndSetStatus(cancelledOrders, OrderStatus.Cancelled)
        orderBooksHolder.removeOrdersFromMapsAndSetStatus(replacedOrders, OrderStatus.Replaced)
    }

    private fun addLimitOrdersInfoToExecutionEventData(orders: Collection<LimitOrder>,
                                                       executionContext: ExecutionContext) {
        orders.forEach { order ->
            val isTrustedClientOrder = applicationSettingsHolder.isTrustedClient(order.clientId)
            if (isTrustedClientOrder && !order.isPartiallyMatched()) {
                executionContext.addTrustedClientLimitOrderWithTrades(LimitOrderWithTrades(order))
            } else {
                executionContext.addClientLimitOrderWithTrades(LimitOrderWithTrades(order))
            }
        }
    }

    private fun processWalletOperationsForCancelledOrders(limitOrders: Collection<LimitOrder>,
                                                          stopLimitOrders: Collection<LimitOrder>,
                                                          executionContext: ExecutionContext,
                                                          allowInvalidBalances: Boolean) {
        val walletOperations = calculateWalletOperationsForCancelledLimitOrders(limitOrders,
                stopLimitOrders,
                executionContext)
        executionContext.walletOperationsProcessor.preProcess(walletOperations, allowInvalidBalances)
    }

    private fun calculateWalletOperationsForCancelledLimitOrders(limitOrders: Collection<LimitOrder>,
                                                                 stopLimitOrders: Collection<LimitOrder>,
                                                                 executionContext: ExecutionContext): List<WalletOperation> {
        val walletOperations = ArrayList<WalletOperation>(limitOrders.size + stopLimitOrders.size)
        walletOperations.addAll(limitOrders.mapNotNull {
            calculateWalletOperationForCancelledOrder(it,
                    executionContext,
                    ::getLimitOrderReservedVolume)
        })
        walletOperations.addAll(stopLimitOrders.mapNotNull {
            calculateWalletOperationForCancelledOrder(it,
                    executionContext,
                    ::getStopOrderReservedVolume)
        })
        return walletOperations
    }

    private fun calculateWalletOperationForCancelledOrder(order: LimitOrder,
                                                          executionContext: ExecutionContext,
                                                          getOrderReservedVolume: (LimitOrder, Asset) -> BigDecimal): WalletOperation? {
        if (applicationSettingsHolder.isTrustedClient(order.clientId)) {
            return null
        }
        val assetPair = executionContext.assetPairsById[order.assetPairId]
        if (assetPair == null) {
            executionContext.info("Reserved balance is not refunded (order: ${order.externalId}) due to not found asset pair ${order.assetPairId}")
            return null
        }
        val limitAssetId = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        val limitAsset = executionContext.assetsById[limitAssetId]!!
        val limitVolume = getOrderReservedVolume(order, limitAsset)
        return WalletOperation(order.clientId,
                limitAssetId,
                BigDecimal.ZERO,
                -limitVolume)
    }

    private fun getLimitOrderReservedVolume(order: LimitOrder, limitAsset: Asset): BigDecimal {
        return order.reservedLimitVolume ?: if (order.isBuySide())
            NumberUtils.setScale(order.getAbsRemainingVolume() * order.price, limitAsset.accuracy, false)
        else
            order.getAbsRemainingVolume()
    }

    private fun getStopOrderReservedVolume(order: LimitOrder, limitAsset: Asset): BigDecimal {
        return order.reservedLimitVolume ?: if (order.isBuySide())
            NumberUtils.setScale(order.volume * (order.upperPrice ?: order.lowerPrice)!!, limitAsset.accuracy, false)
        else
            order.getAbsRemainingVolume()
    }
}