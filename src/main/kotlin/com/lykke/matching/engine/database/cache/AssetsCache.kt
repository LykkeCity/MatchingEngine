package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import kotlin.concurrent.fixedRateTimer

@Component
class AssetsCache @Autowired constructor(
        private val databaseAccessor: BackOfficeDatabaseAccessor,
        updateInterval: Long? = null) : DataCache() {

    companion object {
        val LOGGER = Logger.getLogger(AssetsCache::class.java)
    }

    private var assetsMap: Map<String, Asset>

    fun getAsset(asset: String): Asset? {
        return assetsMap[asset] ?: databaseAccessor.loadAsset(asset)
    }

    override fun update() {
        val newMap = databaseAccessor.loadAssets()
        if (newMap.isNotEmpty()) {
            assetsMap = databaseAccessor.loadAssets()
        }
    }

    init {
        this.assetsMap = databaseAccessor.loadAssets()
        LOGGER.info("Loaded ${assetsMap.size} assets")
        updateInterval?.let {
            fixedRateTimer(name = "Assets Cache Updater", initialDelay = it, period = it) {
                update()
            }
        }
    }
}