package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.AssetPair
import java.util.HashMap

class TestDictionariesDatabaseAccessor : DictionariesDatabaseAccessor {

    private val assetPairs = HashMap<String, AssetPair>()

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        return assetPairs
    }

    override fun loadAssetPair(assetId: String, throwException: Boolean): AssetPair? {
        return assetPairs[assetId]
    }

    fun addAssetPair(pair: AssetPair) {
        assetPairs[pair.assetPairId] = pair
    }

    fun clear() {
        assetPairs.clear()
    }
}