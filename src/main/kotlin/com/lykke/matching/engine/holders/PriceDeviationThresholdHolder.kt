package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.order.transaction.ExecutionContext
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class PriceDeviationThresholdHolder(private val applicationSettingsHolder: ApplicationSettingsHolder) {

    fun getMidPriceDeviationThreshold(assetPairId: String, executionContext: ExecutionContext): BigDecimal? {
        return executionContext.assetPairsById[assetPairId]?.midPriceDeviationThreshold
    }

    fun getMarketOrderPriceDeviationThreshold(assetPairId: String, executionContext: ExecutionContext): BigDecimal? {
        return executionContext.assetPairsById[assetPairId]?.marketOrderPriceDeviationThreshold ?: applicationSettingsHolder.marketOrderPriceDeviationThreshold(assetPairId)
    }
}