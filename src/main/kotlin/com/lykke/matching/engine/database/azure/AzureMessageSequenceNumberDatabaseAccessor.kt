package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.database.ReadOnlyMessageSequenceNumberDatabaseAccessor

class AzureMessageSequenceNumberDatabaseAccessor : ReadOnlyMessageSequenceNumberDatabaseAccessor {
    override fun getSequenceNumber() = 0L
}