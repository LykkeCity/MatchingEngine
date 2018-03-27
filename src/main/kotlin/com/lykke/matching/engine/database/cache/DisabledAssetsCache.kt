package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import kotlin.concurrent.fixedRateTimer

@Component
class DisabledAssetsCache @Autowired constructor (
        private val databaseAccessor: SettingsDatabaseAccessor,
        @Value("\${cache.default.update.interval}") updateInterval: Long) : DataCache() {

    companion object {
        val LOGGER = Logger.getLogger(DisabledAssetsCache::class.java)
    }

    private var disabledPairs: Set<String> = HashSet()

    fun isDisabled(asset: String): Boolean {
        return disabledPairs.contains(asset)
    }

    override fun update() {
       disabledPairs = databaseAccessor.loadDisabledPairs()
    }

    init {
        this.disabledPairs = databaseAccessor.loadDisabledPairs()
        LOGGER.info("Loaded ${disabledPairs.size} disabled assets")
        updateInterval.let {
            fixedRateTimer(name = "Disabled Assets Cache Updater", initialDelay = it, period = it) {
                update()
            }
        }
    }
}