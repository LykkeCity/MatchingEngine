package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.services.validators.common.OrderValidationUtils
import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

class MidPriceUtils {
    companion object {
        fun getMidPricesInterval(threshold: BigDecimal?, referenceMidPrice: BigDecimal): Pair<BigDecimal?, BigDecimal?> {
            var lowerMidPriceBound: BigDecimal? = null
            var upperMidPriceBound: BigDecimal? = null
            if (threshold != null && !NumberUtils.equalsIgnoreScale(referenceMidPrice, BigDecimal.ZERO)) {
                lowerMidPriceBound = referenceMidPrice - (referenceMidPrice * threshold)
                upperMidPriceBound = referenceMidPrice + (referenceMidPrice * threshold)
            }
            return lowerMidPriceBound to upperMidPriceBound
        }

        fun isMidPriceValid(threshold: BigDecimal?,
                            referenceMidPrice: BigDecimal,
                            assetPairId: String,
                            executionContext: ExecutionContext): Boolean {
            val (lowerBound, upperBound) = getMidPricesInterval(threshold, referenceMidPrice)
            val midPrice = executionContext.orderBooksHolder.getChangedCopyOrOriginalOrderBook(assetPairId).getMidPrice()
            return OrderValidationUtils.isMidPriceValid(midPrice, lowerBound, upperBound)
        }
    }
}