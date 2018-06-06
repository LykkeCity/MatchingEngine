package com.lykke.matching.engine.database

import com.lykke.matching.engine.deduplication.ProcessedMessage

interface ReadOnlyProcessedMessagesDatabaseAccessor {
    fun get(): Set<ProcessedMessage>
}