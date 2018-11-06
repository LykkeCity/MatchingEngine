package com.lykke.matching.engine.utils.monitoring

import com.lykke.matching.engine.common.events.RefMidPriceDangerousChangeEvent
import com.lykke.matching.engine.holders.PriceDeviationThresholdHolder
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.utils.logging.MetricsLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class OrderBookMidPriceChecker {
    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    private lateinit var priceDeviationThresholdHolder: PriceDeviationThresholdHolder

    @EventListener
    fun processReferencePriceReadyEvent(referencePriceReadyEvent: RefMidPriceDangerousChangeEvent) {
        val assetPairId = referencePriceReadyEvent.assetPairId
        val orderBook = genericLimitOrderService.getOrderBook(assetPairId)
        val midPrice = orderBook.getMidPrice()
        val threshold = priceDeviationThresholdHolder.getMidPriceDeviationThreshold(assetPairId) ?: return

        val lowerBound = getLowerBound(referencePriceReadyEvent.refMidPrice, threshold)
        val upperBound = getUpperBound(referencePriceReadyEvent.refMidPrice, threshold)
        if (!OrderValidationUtils.isMidPriceValid(midPrice, lowerBound, upperBound)) {
            METRICS_LOGGER.logError("Order book ${orderBook.assetPairId}, mid price $midPrice is out of reference mid price range " +
                    "lowerBound $lowerBound, upperBound: $upperBound, event is triggered by orders cancel change: ${referencePriceReadyEvent.cancel}" +
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