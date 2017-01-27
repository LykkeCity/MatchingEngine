package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import org.apache.log4j.Logger
import kotlin.concurrent.fixedRateTimer

class AssetsCache(
        private val databaseAccessor: BackOfficeDatabaseAccessor,
        updateInterval: Long) : DataCache() {

    companion object {
        val LOGGER = Logger.getLogger(AssetsCache::class.java)
    }

    private var assetsMap: Map<String, Asset>

    fun getAsset(asset: String): Asset? {
        return assetsMap[asset] ?: databaseAccessor.loadAsset(asset)
    }

    override fun update() {
        assetsMap = databaseAccessor.loadAssets()
    }

    init {
        this.assetsMap = databaseAccessor.loadAssets()
        LOGGER.info("Loaded ${assetsMap.size} assets")
        fixedRateTimer(name = "Assets Cache Updater", initialDelay = updateInterval, period = updateInterval) {
            update()
        }
    }
}