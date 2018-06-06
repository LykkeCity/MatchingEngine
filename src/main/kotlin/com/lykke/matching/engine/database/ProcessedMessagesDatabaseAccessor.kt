package com.lykke.matching.engine.database

import com.lykke.matching.engine.deduplication.ProcessedMessage

interface ProcessedMessagesDatabaseAccessor: ReadOnlyProcessedMessagesDatabaseAccessor {
    fun saveProcessedMessage(message: ProcessedMessage)
}