package com.lykke.matching.engine.utils.monitoring

import com.lykke.matching.engine.common.events.RefMidPriceDangerousChangeEvent
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.utils.logging.MetricsLogger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class OrderBookMidPriceChecker {
    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private val LOGGER = LoggerFactory.getLogger(OrderBookMidPriceChecker::class.java)
    }

    fun checkOrderBook(refMidPriceDangerousChangeEvent: RefMidPriceDangerousChangeEvent) {
        val assetPairId = refMidPriceDangerousChangeEvent.assetPairId
        val executionContext = refMidPriceDangerousChangeEvent.executionContext
        val orderBook = executionContext.orderBooksHolder.getOrderBook(assetPairId)
        val midPrice = orderBook.getMidPrice()
        val threshold = executionContext.assetPairsById[assetPairId]?.midPriceDeviationThreshold ?: return

        val lowerBound = getLowerBound(refMidPriceDangerousChangeEvent.refMidPrice, threshold)
        val upperBound = getUpperBound(refMidPriceDangerousChangeEvent.refMidPrice, threshold)
        if (!OrderValidationUtils.isMidPriceValid(midPrice, lowerBound, upperBound)) {
            val message = "Order book ${orderBook.assetPairId}, mid price $midPrice is out of reference mid price range " +
                    "lowerBound $lowerBound, upperBound: $upperBound, event is triggered by orders cancel change: ${executionContext.executionContextForCancelOperation}" +
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