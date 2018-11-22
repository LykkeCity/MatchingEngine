package com.lykke.matching.engine.daos

import java.io.Serializable

data class Asset(
        val assetId: String,
        val accuracy: Int
): Serializable {
    override fun toString(): String {
        return "Asset(" +
                "assetId='$assetId', " +
                "accuracy=$accuracy)"
    }
}