package com.lykke.matching.engine.database

import com.lykke.matching.engine.deduplication.ProcessedMessage
import java.time.LocalDate

interface ProcessedMessagesDatabaseAccessor {
    fun loadProcessedMessages(startDate: LocalDate): List<ProcessedMessage>
    fun saveProcessedMessage(message: ProcessedMessage)
}