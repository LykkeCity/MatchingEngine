package com.lykke.matching.engine.daos

class AssetPair(
        val assetPairId: String,
        val baseAssetId: String,
        val quotingAssetId: String,
        val accuracy: Int
) {
    override fun toString(): String {
        return "AssetPair(" +
                "assetPairId='$assetPairId', " +
                "baseAssetId='$baseAssetId', " +
                "quotingAssetId='$quotingAssetId', " +
                "accuracy=$accuracy"
    }
}