package com.lykke.matching.engine.database

class TestConfigDatabaseAccessor: ConfigDatabaseAccessor {

    private val configs = HashMap<String, String>()

    override fun loadConfigs(): Map<String, String>? = HashMap(configs)

    fun setProperty(name: String, value: String) {
        configs[name] = value
    }

}