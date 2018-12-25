package com.lykke.matching.engine.daos

class Asset(
        val assetId: String,
        val accuracy: Int
){
    override fun toString(): String {
        return "Asset(" +
                "assetId='$assetId', " +
                "accuracy=$accuracy)"
    }
}