package com.lykke.matching.engine.daos

import java.math.BigDecimal

class AssetPair(
        val assetPairId: String,
        val baseAssetId: String,
        val quotingAssetId: String,
        val accuracy: Int,
        val minVolume: BigDecimal? = null,
        val minInvertedVolume: BigDecimal? = null,
        val maxValue: BigDecimal? = null,
        val midPriceDeviationThreshold: BigDecimal? = null,
        val marketOrderPriceDeviationThreshold: BigDecimal? = null
) {
    override fun toString(): String {
        return "AssetPair(" +
                "assetPairId='$assetPairId', " +
                "baseAssetId='$baseAssetId', " +
                "quotingAssetId='$quotingAssetId', " +
                "accuracy=$accuracy, " +
                "minVolume=$minVolume, " +
                "minInvertedVolume=$minInvertedVolume, " +
                "maxValue=$maxValue, " +
                "midPriceDeviationThreshold=$midPriceDeviationThreshold, " +
                "marketOrderPriceDeviationThreshold=$marketOrderPriceDeviationThreshold"
    }
}