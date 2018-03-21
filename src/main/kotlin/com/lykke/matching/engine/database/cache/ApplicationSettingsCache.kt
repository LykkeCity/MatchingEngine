package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.Settings
import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import kotlin.concurrent.fixedRateTimer

class ApplicationSettingsCache(private val configDatabaseAccessor: ConfigDatabaseAccessor,
                               updateInterval: Long? = null) : DataCache() {


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
        configDatabaseAccessor.loadConfigs()?.let { settings = it }
    }

    val trustedClients: Set<String>
    get() = this.settings.trustedClients

    fun isAssetDisabled(asset: String): Boolean {
        return this.settings.disabledAssets.contains(asset)
    }
}