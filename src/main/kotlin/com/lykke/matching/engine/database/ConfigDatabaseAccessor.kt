package com.lykke.matching.engine.database

interface ConfigDatabaseAccessor {
    fun loadConfigs(): Map<String, String>?
    fun saveValue(name: String, value: String)
}