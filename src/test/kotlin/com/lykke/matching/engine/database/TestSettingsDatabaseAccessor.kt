package com.lykke.matching.engine.database

class TestSettingsDatabaseAccessor : ConfigDatabaseAccessor {
    private val  settings = HashMap<String, MutableSet<String>>()

    companion object {
        private val TRUSTED_CLIENTS = "TrustedClients"
    }

    init {
        settings[TRUSTED_CLIENTS] = HashSet()
    }

    override fun loadConfigs(): Map<String, Set<String>> {
        return settings
    }

    fun addTrustedClient(trustedClient: String) {
        settings[TRUSTED_CLIENTS]?.add(trustedClient)
    }
}