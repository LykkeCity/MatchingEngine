package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.outgoing.messages.LimitOrderWithTrades
import java.math.BigDecimal
import java.util.LinkedList

data class CancelledOrdersOperationsResult(
        val walletOperations: List<WalletOperation> = LinkedList(),
        val clientLimitOrderWithTrades: List<LimitOrderWithTrades> = LinkedList(),
        val trustedClientLimitOrderWithTrades: List<LimitOrderWithTrades> = LinkedList()
)

class WalletOperationsCalculator(
        private val assetsPairsHolder: AssetsPairsHolder,
        private val balancesHolder: BalancesHolder,
        private val applicationSettingsHolder: ApplicationSettingsHolder
) {

    fun calculateForCancelledOrders(orders: List<LimitOrder>): CancelledOrdersOperationsResult {
        val walletOperation = LinkedList<WalletOperation>()
        val trustedLimitOrderWithTrades = LinkedList<LimitOrderWithTrades>()
        val limitOrderWithTrades = LinkedList<LimitOrderWithTrades>()

        orders.forEach { order ->
            val isTrustedClientOrder = applicationSettingsHolder.isTrustedClient(order.clientId)

            if (!isTrustedClientOrder) {
                val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
                val limitAsset = if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
                val limitVolume = order.reservedLimitVolume ?: if (order.isBuySide()) order.getAbsRemainingVolume() * order.price else order.getAbsRemainingVolume()
                val reservedBalance = balancesHolder.getReservedBalance(order.clientId, limitAsset)

                if (reservedBalance > BigDecimal.ZERO) {
                    walletOperation.add(
                            WalletOperation(order.clientId, limitAsset, BigDecimal.ZERO, if (limitVolume > reservedBalance) -reservedBalance else -limitVolume)
                    )
                }
            }

            if (isTrustedClientOrder && !order.isPartiallyMatched()) {
                limitOrderWithTrades.add(LimitOrderWithTrades(order))
            } else {
                trustedLimitOrderWithTrades.add(LimitOrderWithTrades(order))
            }
        }

        return CancelledOrdersOperationsResult(walletOperation, trustedLimitOrderWithTrades, limitOrderWithTrades)
    }

}