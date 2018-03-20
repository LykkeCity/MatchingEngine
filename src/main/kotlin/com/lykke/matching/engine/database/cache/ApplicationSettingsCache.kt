package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.database.ConfigDatabaseAccessor
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

    private lateinit var settings: Map<String, Set<String>>

    override fun update() {
        configDatabaseAccessor.loadConfigs()?.let { settings = it}
    }

    val trustedClients: Set<String>
    get() = this.settings[TRUSTED_CLIENTS]?.toMutableSet() ?: HashSet()

    fun isAssetDisabled(asset: String): Boolean {
        return this.settings[DISABLED_ASSETS]?.contains(asset) ?: false
    }
}