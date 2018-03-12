package com.lykke.matching.engine.database

interface SettingsDatabaseAccessor {
    fun loadDisabledPairs(): Set<String>
}