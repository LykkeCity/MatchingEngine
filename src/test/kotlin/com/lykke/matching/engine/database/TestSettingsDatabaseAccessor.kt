package com.lykke.matching.engine.database

class TestSettingsDatabaseAccessor : ConfigDatabaseAccessor {
    override fun loadConfigs(): Map<String, Set<String>> {
        return emptyMap()
    }
}