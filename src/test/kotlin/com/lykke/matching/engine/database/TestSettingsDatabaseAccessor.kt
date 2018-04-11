package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Settings

class TestSettingsDatabaseAccessor : ConfigDatabaseAccessor {
    private var settings = Settings()

    override fun loadConfigs(): Settings {
        return settings
    }

    fun addTrustedClient(trustedClient: String) {
        val trustedClients: MutableSet<String> = HashSet(settings.trustedClients)
        trustedClients.add(trustedClient)

        settings = Settings(trustedClients = trustedClients)
    }

    fun clear() {
        settings = Settings(emptySet(), emptySet())
    }
}