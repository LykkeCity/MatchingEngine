package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import org.apache.log4j.Logger
import kotlin.concurrent.fixedRateTimer

class AssetPairsCache(
        private val databaseAccessor: WalletDatabaseAccessor,
        updateInterval: Long) : DataCache() {

    companion object {
        val LOGGER = Logger.getLogger(AssetPairsCache::class.java)
    }

    private var assetPairsMap: Map<String, AssetPair>

    fun getAssetPair(assetPair: String): AssetPair? {
        return assetPairsMap[assetPair] ?: databaseAccessor.loadAssetPair(assetPair)
    }

    override fun update() {
        assetPairsMap = databaseAccessor.loadAssetPairs()
    }

    init {
        this.assetPairsMap = databaseAccessor.loadAssetPairs()
        fixedRateTimer(name = "Asset Pairs Cache Updater", initialDelay = updateInterval, period = updateInterval) {
            update()
        }
    }
}