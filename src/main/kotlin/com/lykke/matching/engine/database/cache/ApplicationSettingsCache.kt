package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import com.lykke.matching.engine.utils.config.Settings
import kotlin.concurrent.fixedRateTimer

class ApplicationSettingsCache(private val configDatabaseAccessor: ConfigDatabaseAccessor,
                               updateInterval: Long? = null) : DataCache() {
    companion object {
        private val DISABLED_ASSETS = "DisabledAssets"
        private val TRUSTED_CLIENTS = "TrustedClients"
    }

    init {
        update()
        if (updateInterval != null) {
            fixedRateTimer(name = "Application Properties Updater", initialDelay = updateInterval, period = updateInterval) {
                update()
            }
        }
    }

    private var settings: Settings = Settings()

    override fun update() {
        configDatabaseAccessor.loadConfigs()?.let { settings = toSettings(it)}
    }

    fun getSettings(): Settings {
        return this.settings
    }

    private fun toSettings(settingsMap: Map<String, Set<String>>): Settings {
        return Settings(settingsMap[DISABLED_ASSETS], settingsMap[TRUSTED_CLIENTS])
    }
}