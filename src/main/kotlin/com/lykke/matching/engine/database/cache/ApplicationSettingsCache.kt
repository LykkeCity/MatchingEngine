package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.Settings
import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import kotlin.concurrent.fixedRateTimer

@Component
class ApplicationSettingsCache @Autowired constructor (private val configDatabaseAccessor: ConfigDatabaseAccessor,
                                                       @Value("\${application.settings.update.interval}") updateInterval: Long) : DataCache() {

    private var settings: Settings = Settings()

    init {
        update()
        if (updateInterval != null) {
            fixedRateTimer(name = "Application Properties Updater", initialDelay = updateInterval, period = updateInterval, daemon = true) {
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