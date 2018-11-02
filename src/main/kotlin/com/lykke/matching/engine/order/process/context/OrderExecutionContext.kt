package com.lykke.matching.engine.order.process.context

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.matching.MatchingResult
import com.lykke.matching.engine.order.transaction.ExecutionContext
import java.math.BigDecimal

abstract class OrderExecutionContext<T: Order>(val order: T,
                                               val executionContext: ExecutionContext) {
    val oppositeLimitAsset: Asset? = if (order.isBuySide()) {
        executionContext.assetPairsById[order.assetPairId]?.let { executionContext.assetsById[it.baseAssetId] }
    } else {
        executionContext.assetPairsById[order.assetPairId]?.let { executionContext.assetsById[it.quotingAssetId] }
    }

    var lowerMidPriceBound: BigDecimal? = null
    var upperMidPriceBound: BigDecimal? = null
    var matchingResult: MatchingResult? = null
    var cancelledOppositeClientsOrders: List<LimitOrder>? = null
    var cancelledOppositeTrustedClientsOrders: List<LimitOrder>? = null
    var cancelledOppositeOrdersWalletOperations: MutableList<WalletOperation>? = null
    var ownWalletOperations: MutableList<WalletOperation>? = null
    var isUncompletedOrderCancelled = false
}