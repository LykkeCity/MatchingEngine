package com.lykke.matching.engine.utils.monitoring

import com.lykke.matching.engine.common.events.RefMidPriceDangerousChangeEvent
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.utils.logging.MetricsLogger
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class OrderBookMidPriceChecker(val genericLimitOrderService: GenericLimitOrderService,
                               val assetPairsCache: AssetPairsCache) {
    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    fun checkOrderBook(refMidPriceDangerousChangeEvent: RefMidPriceDangerousChangeEvent) {
        val assetPairId = refMidPriceDangerousChangeEvent.assetPairId
        val orderBook = genericLimitOrderService.getOrderBook(assetPairId)
        val midPrice = orderBook.getMidPrice()
        val threshold = assetPairsCache.getAssetPair(assetPairId)?.midPriceDeviationThreshold ?: return

        val lowerBound = getLowerBound(refMidPriceDangerousChangeEvent.refMidPrice, threshold)
        val upperBound = getUpperBound(refMidPriceDangerousChangeEvent.refMidPrice, threshold)
        if (!OrderValidationUtils.isMidPriceValid(midPrice, lowerBound, upperBound)) {
            METRICS_LOGGER.logError("Order book ${orderBook.assetPairId}, mid price $midPrice is out of reference mid price range " +
                    "lowerBound $lowerBound, upperBound: $upperBound, event is triggered by orders cancel change: ${refMidPriceDangerousChangeEvent.cancel}" +
                    " All market orders and taker limit orders will be rejected")
        }
    }

    private fun getLowerBound(midPrice: BigDecimal, threshold: BigDecimal): BigDecimal {
        return midPrice - midPrice * threshold
    }

    private fun getUpperBound(midPrice: BigDecimal, threshold: BigDecimal): BigDecimal {
        return midPrice + midPrice * threshold
    }
}