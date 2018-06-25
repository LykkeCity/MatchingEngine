package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

class AssetPair(
        val assetPairId: String,
        val baseAssetId: String,
        val quotingAssetId: String,
        val accuracy: Int,
        val minVolume: BigDecimal? = null,
        val minInvertedVolume: BigDecimal? = null
) {
    override fun toString(): String {
        return "AssetPair(" +
                "assetPairId='$assetPairId', " +
                "baseAssetId='$baseAssetId', " +
                "quotingAssetId='$quotingAssetId', " +
                "accuracy=$accuracy, " +
                "minVolume=$minVolume, " +
                "minInvertedVolume=$minInvertedVolume"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AssetPair

        if (assetPairId != other.assetPairId) return false
        if (baseAssetId != other.baseAssetId) return false
        if (quotingAssetId != other.quotingAssetId) return false
        if (accuracy != other.accuracy) return false
        if (!NumberUtils.equalsIgnoreScale(minVolume,  other.minVolume)) return false
        if (!NumberUtils.equalsIgnoreScale(minInvertedVolume, other.minInvertedVolume)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = assetPairId.hashCode()
        result = 31 * result + baseAssetId.hashCode()
        result = 31 * result + quotingAssetId.hashCode()
        result = 31 * result + accuracy
        result = 31 * result + (minVolume?.stripTrailingZeros()?.hashCode() ?: 0)
        result = 31 * result + (minInvertedVolume?.stripTrailingZeros()?.hashCode() ?: 0)
        return result
    }
}