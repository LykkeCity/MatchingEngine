package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.Settings
import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import kotlin.concurrent.fixedRateTimer

class ApplicationSettingsCache(private val configDatabaseAccessor: ConfigDatabaseAccessor,
                               updateInterval: Long? = null) : DataCache() {


    private var settings: Settings = Settings()

    init {
        update()
        if (updateInterval != null) {
            fixedRateTimer(name = "Application Properties Updater", initialDelay = updateInterval, period = updateInterval) {
                update()
            }
        }
    }

    override fun update() {
        configDatabaseAccessor.loadConfigs()?.let { settings = it }
    }

    fun isTrustedClient(client: String): Boolean {
        return this.settings.trustedClients.contains(client)
    }

    fun isAssetDisabled(asset: String): Boolean {
        return this.settings.disabledAssets.contains(asset)
    }
}