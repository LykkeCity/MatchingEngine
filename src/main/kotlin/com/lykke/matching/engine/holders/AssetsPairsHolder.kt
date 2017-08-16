package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.cache.AssetPairsCache

class AssetsPairsHolder(private val assetPairsCache: AssetPairsCache) {
    fun getAssetPair(assetPairId: String): AssetPair {
        val assetPair = assetPairsCache.getAssetPair(assetPairId) ?: throw Exception("Unable to find asset pair $assetPairId")
        return assetPair
    }
}