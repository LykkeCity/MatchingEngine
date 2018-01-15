package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.cache.AssetPairsCache

class AssetsPairsHolder(private val assetPairsCache: AssetPairsCache) {
    fun getAssetPair(assetPairId: String): AssetPair {
        return assetPairsCache.getAssetPair(assetPairId) ?: throw Exception("Unable to find asset pair $assetPairId")
    }

    fun getAssetPair(assetId1: String, assetId2: String): AssetPair {
        return assetPairsCache.getAssetPair(assetId1, assetId2) ?: throw Exception("Unable to find asset pair for ($assetId1 & $assetId2)")
    }
}