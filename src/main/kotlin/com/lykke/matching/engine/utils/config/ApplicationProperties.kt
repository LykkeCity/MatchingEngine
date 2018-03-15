package com.lykke.matching.engine.utils.config

import com.lykke.matching.engine.database.azure.AzureConfigDatabaseAccessor
import com.lykke.matching.engine.database.cache.DataCache
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import kotlin.concurrent.fixedRateTimer

class ApplicationProperties(private val configDatabaseAccessor: AzureConfigDatabaseAccessor,
                            updateInterval: Long? = null) : DataCache() {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(ApplicationProperties::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()

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


    private fun toSettings(settingsMap: Map<String, Set<String>>): Settings {
        return Settings(settingsMap[DISABLED_ASSETS],settingsMap[TRUSTED_CLIENTS])
    }
}