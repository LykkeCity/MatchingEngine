package com.lykke.matching.engine.holders
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class PriceDeviationThresholdHolder(private val settingsCache: ApplicationSettingsCache) {
    fun getMidPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return settingsCache.midPriceDeviationThreshold(assetPairId)
    }
    fun getMarketOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return settingsCache.marketOrderPriceDeviationThreshold(assetPairId)
    }
}