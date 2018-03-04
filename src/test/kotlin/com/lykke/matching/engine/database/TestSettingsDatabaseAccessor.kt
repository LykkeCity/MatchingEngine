package com.lykke.matching.engine.database

class TestSettingsDatabaseAccessor : SettingsDatabaseAccessor {
    override fun loadDisabledPairs(): Set<String> {
        return emptySet()
    }
}