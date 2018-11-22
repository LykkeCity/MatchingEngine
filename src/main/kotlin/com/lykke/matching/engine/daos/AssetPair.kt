package com.lykke.matching.engine.daos

import java.io.Serializable
import java.math.BigDecimal

data class AssetPair(
        val assetPairId: String,
        val baseAssetId: String,
        val quotingAssetId: String,
        val accuracy: Int,
        val minVolume: BigDecimal? = null,
        val minInvertedVolume: BigDecimal? = null,
        val maxVolume: BigDecimal? = null,
        val maxValue: BigDecimal? = null
): Serializable {
    override fun toString(): String {
        return "AssetPair(" +
                "assetPairId='$assetPairId', " +
                "baseAssetId='$baseAssetId', " +
                "quotingAssetId='$quotingAssetId', " +
                "accuracy=$accuracy, " +
                "minVolume=$minVolume, " +
                "minInvertedVolume=$minInvertedVolume, " +
                "maxVolume=$maxVolume, " +
                "maxValue=$maxValue"
    }
}