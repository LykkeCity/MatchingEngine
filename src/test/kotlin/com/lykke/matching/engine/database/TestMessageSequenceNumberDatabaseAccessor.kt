package com.lykke.matching.engine.database

class TestMessageSequenceNumberDatabaseAccessor : ReadOnlyMessageSequenceNumberDatabaseAccessor {
    override fun getSequenceNumber() = 0L
}