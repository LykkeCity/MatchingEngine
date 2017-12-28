package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import org.apache.log4j.Logger
import kotlin.concurrent.fixedRateTimer

class AssetPairsCache(
        private val databaseAccessor: DictionariesDatabaseAccessor,
        updateInterval: Long) : DataCache() {

    companion object {
        private val LOGGER = Logger.getLogger(AssetPairsCache::class.java)
    }

    private var assetPairsById: Map<String, AssetPair>
    private var assetPairsByPair: Map<String, AssetPair>

    fun getAssetPair(assetPair: String): AssetPair? {
        return assetPairsById[assetPair] ?: databaseAccessor.loadAssetPair(assetPair)
    }

    fun getAssetPair(assetId1: String, assetId2: String): AssetPair? {
        return assetPairsByPair[pairKey(assetId1, assetId2)] ?: assetPairsByPair[pairKey(assetId2, assetId1)]
    }

    override fun update() {
        val newMap = databaseAccessor.loadAssetPairs()
        if (newMap.isNotEmpty()) {
            val newMapByPair = generateAssetPairsMapByPair(newMap)
            assetPairsById = newMap
            assetPairsByPair = newMapByPair
        }
    }

    private fun generateAssetPairsMapByPair(assetPairsById: Map<String, AssetPair>): Map<String, AssetPair> {
        return assetPairsById.values
                .groupBy { pairKey(it.baseAssetId, it.quotingAssetId) }
                .mapValues {
                    if (it.value.size > 1) {
                        LOGGER.error("Asset pairs count for baseAssetId=${it.value.first().baseAssetId} and quotingAssetId=${it.value.first().quotingAssetId} is more than 1")
                    }
                    it.value.first()
                }
    }

    private fun pairKey(assetId1: String, assetId2: String) = "${assetId1}_$assetId2"

    init {
        val assetPairsById = databaseAccessor.loadAssetPairs()
        val assetPairsByPair = generateAssetPairsMapByPair(assetPairsById)
        this.assetPairsById = assetPairsById
        this.assetPairsByPair = assetPairsByPair
        LOGGER.info("Loaded ${assetPairsById.size} assets pairs")
        fixedRateTimer(name = "Asset Pairs Cache Updater", initialDelay = updateInterval, period = updateInterval) {
            update()
        }
    }
}