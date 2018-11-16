package com.lykke.matching.engine.utils.monitoring

import com.lykke.matching.engine.common.events.RefMidPriceDangerousChangeEvent
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class OrderBookMidPriceChecker(private val genericLimitOrderService: GenericLimitOrderService,
                               private val priceDeviationThresholdHolder: PriceDeviationThresholdHolder) {
    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private val LOGGER = Logger.getLogger(OrderBookMidPriceChecker::class.java)
    }

    fun checkOrderBook(referencePriceReadyEvent: RefMidPriceDangerousChangeEvent) {
        val assetPairId = referencePriceReadyEvent.assetPairId
        val orderBook = genericLimitOrderService.getOrderBook(assetPairId)
        val midPrice = orderBook.getMidPrice()
        val threshold = priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPairId) ?: return

        val lowerBound = getLowerBound(referencePriceReadyEvent.refMidPrice, threshold)
        val upperBound = getUpperBound(referencePriceReadyEvent.refMidPrice, threshold)
        if (!OrderValidationUtils.isMidPriceValid(midPrice, lowerBound, upperBound)) {
            val message = "Order book ${orderBook.assetPairId}, mid price $midPrice is out of reference mid price range " +
                    "lowerBound $lowerBound, upperBound: $upperBound, event is triggered by orders cancel change: ${referencePriceReadyEvent.cancel}" +
                    " All market orders and taker limit orders will be rejected"
            LOGGER.warn(message)
            METRICS_LOGGER.logError(message)
        }
    }

    private fun getLowerBound(midPrice: BigDecimal, threshold: BigDecimal): BigDecimal {
        return midPrice - midPrice * threshold
    }

    private fun getUpperBound(midPrice: BigDecimal, threshold: BigDecimal): BigDecimal {
        return midPrice + midPrice * threshold
    }
}