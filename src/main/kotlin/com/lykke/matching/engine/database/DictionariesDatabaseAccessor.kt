package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.AssetPair
import java.util.HashMap

interface DictionariesDatabaseAccessor {
    fun loadAssetPairs(): HashMap<String, AssetPair>
    fun loadAssetPair(assetId: String, throwException: Boolean = false): AssetPair?
}