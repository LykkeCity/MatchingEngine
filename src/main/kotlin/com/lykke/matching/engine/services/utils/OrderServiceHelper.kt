package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.matching.MatchingResult
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.NotEnoughFundsLimitOrderCancelParams
import com.lykke.utils.number.RoundingUtils
import org.apache.log4j.Logger

class OrderServiceHelper(private val genericLimitOrderService: GenericLimitOrderService,
                         private val LOGGER: Logger) {

    fun processUncompletedOrder(matchingResult: MatchingResult, assetPair: AssetPair, walletOperations: MutableList<WalletOperation>) {
        val uncompletedLimitOrder = matchingResult.uncompletedLimitOrder ?: return
        if (assetPair.minVolume != null && uncompletedLimitOrder.getAbsRemainingVolume() < assetPair.minVolume) {
            val result = genericLimitOrderService.cancelNotEnoughFundsOrder(NotEnoughFundsLimitOrderCancelParams(listOf(uncompletedLimitOrder)))
            walletOperations.addAll(result.walletOperation)
            LOGGER.info("Order (id: ${uncompletedLimitOrder.externalId}) is cancelled due to min remaining volume (${RoundingUtils.roundForPrint(uncompletedLimitOrder.getAbsRemainingVolume())} < ${RoundingUtils.roundForPrint(assetPair.minVolume)})")
        } else {
            matchingResult.orderBook.put(uncompletedLimitOrder)
        }
    }
}