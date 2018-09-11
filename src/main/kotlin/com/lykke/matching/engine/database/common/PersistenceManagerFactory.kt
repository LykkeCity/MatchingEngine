package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.database.PersistenceManager

@FunctionalInterface
interface PersistenceManagerFactory {
    fun get(connectionName: String): PersistenceManager
}