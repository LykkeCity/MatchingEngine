package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.database.MessageSequenceNumberDatabaseAccessor

class AzureMessageSequenceNumberDatabaseAccessor : MessageSequenceNumberDatabaseAccessor {
    override fun getSequenceNumber() = 0L
}