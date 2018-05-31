package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Settings

class TestConfigDatabaseAccessor : ConfigDatabaseAccessor {
    private var settings = Settings()

    override fun loadConfigs(): Settings {
        return settings
    }

    fun addTrustedClient(trustedClient: String) {
        val trustedClients: MutableSet<String> = HashSet(settings.trustedClients)
        trustedClients.add(trustedClient)

        settings = Settings(trustedClients = trustedClients,
                disabledAssets = settings.disabledAssets)
    }

    fun addDisabledAsset(disabledAsset: String) {
        val disabledAssets: MutableSet<String> = HashSet(settings.disabledAssets)
        disabledAssets.add(disabledAsset)

        settings = Settings(trustedClients = settings.trustedClients,
                disabledAssets = disabledAssets)
    }

    fun clear() {
        settings = Settings(emptySet(), emptySet())
    }
}