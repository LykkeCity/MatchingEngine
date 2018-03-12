package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import org.apache.log4j.Logger
import kotlin.concurrent.fixedRateTimer

class DisabledAssetsCache(
        private val databaseAccessor: SettingsDatabaseAccessor,
        updateInterval: Long? = null) : DataCache() {

    companion object {
        val LOGGER = Logger.getLogger(DisabledAssetsCache::class.java)
    }

    private var disabledPairs: Set<String>

    fun isDisabled(asset: String): Boolean {
        return disabledPairs.contains(asset)
    }

    override fun update() {
       disabledPairs = databaseAccessor.loadDisabledPairs()
    }

    init {
        this.disabledPairs = databaseAccessor.loadDisabledPairs()
        LOGGER.info("Loaded ${disabledPairs.size} disabled assets")
        updateInterval?.let {
            fixedRateTimer(name = "Disabled Assets Cache Updater", initialDelay = it, period = it) {
                update()
            }
        }
    }
}