package com.lykke.matching.engine.database

import com.lykke.matching.engine.deduplication.ProcessedMessage
import java.util.Date

interface ProcessedMessagesDatabaseAccessor {
    fun loadProcessedMessages(startDate: Date): List<ProcessedMessage>
    fun saveProcessedMessage(message: ProcessedMessage)
}