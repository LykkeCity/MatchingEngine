package com.lykke.matching.engine.database

interface ReadOnlyMessageSequenceNumberDatabaseAccessor {
    fun getSequenceNumber(): Long
}