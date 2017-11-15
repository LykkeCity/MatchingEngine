package com.lykke.matching.engine.database

interface MessageLogDatabaseAccessor<in T> {
    fun log(message: T)
}