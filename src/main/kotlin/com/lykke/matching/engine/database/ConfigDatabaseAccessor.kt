package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Settings

interface ConfigDatabaseAccessor {
    fun loadConfigs(): Settings?
}