package com.lykke.matching.engine.database

class TestConfigDatabaseAccessor: ConfigDatabaseAccessor {

    private val configs = HashMap<String, Set<String>>()

    override fun loadConfigs(): Map<String, Set<String>>? = HashMap(configs)
}