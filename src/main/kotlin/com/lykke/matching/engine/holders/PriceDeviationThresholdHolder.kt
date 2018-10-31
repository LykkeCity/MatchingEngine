package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class PriceDeviationThresholdHolder(private val assetsPairsCache: AssetPairsCache,
                                    private val settingsCache: ApplicationSettingsCache) {

    fun getLimitOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return assetsPairsCache.getAssetPair(assetPairId)?.limitOrderPriceDeviationThreshold ?: settingsCache.limitOrderPriceDeviationThreshold(assetPairId)
    }

    fun getMarketOrderPriceDeviationThreshold(assetPairId: String): BigDecimal? {
        return assetsPairsCache.getAssetPair(assetPairId)?.marketOrderPriceDeviationThreshold ?: settingsCache.marketOrderPriceDeviationThreshold(assetPairId)
    }
}