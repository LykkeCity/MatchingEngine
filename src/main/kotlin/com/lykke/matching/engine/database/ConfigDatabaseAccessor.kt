package com.lykke.matching.engine.database

interface ConfigDatabaseAccessor {
    fun loadConfigs(): Map<String, Set<String>>?
}