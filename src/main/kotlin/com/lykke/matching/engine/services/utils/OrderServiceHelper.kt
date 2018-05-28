package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.matching.MatchingResult
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger

class OrderServiceHelper(private val genericLimitOrderService: GenericLimitOrderService,
                         private val LOGGER: Logger) {

    fun preProcessUncompletedOrder(matchingResult: MatchingResult, assetPair: AssetPair, walletOperations: MutableList<WalletOperation>): Boolean {
        val uncompletedLimitOrder = matchingResult.uncompletedLimitOrderCopy ?: return false
        if (assetPair.minVolume != null && uncompletedLimitOrder.getAbsRemainingVolume() < assetPair.minVolume) {
            val result = genericLimitOrderService.calculateWalletOperationsForCancelledOrders(listOf(uncompletedLimitOrder))
            walletOperations.addAll(result.walletOperations)
            LOGGER.info("Order (id: ${uncompletedLimitOrder.externalId}) is cancelled due to min remaining volume (${NumberUtils.roundForPrint(uncompletedLimitOrder.getAbsRemainingVolume())} < ${NumberUtils.roundForPrint(assetPair.minVolume)})")
            return false
        }
        return true
    }

    fun processUncompletedOrder(matchingResult: MatchingResult, preProcessUncompletedOrder: Boolean) {
        val uncompletedLimitOrder = matchingResult.uncompletedLimitOrder ?: return
        if (!preProcessUncompletedOrder) {
            genericLimitOrderService.cancelLimitOrders(listOf(uncompletedLimitOrder))
            return
        }
        matchingResult.orderBook.put(uncompletedLimitOrder)
    }
}