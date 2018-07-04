package com.lykke.matching.engine.database

interface MessageSequenceNumberDatabaseAccessor {
    fun getSequenceNumber(): Long
}