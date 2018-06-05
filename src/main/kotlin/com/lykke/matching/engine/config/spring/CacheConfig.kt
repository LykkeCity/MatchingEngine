package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.ReadOnlyProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class CacheConfig {

    @Autowired
    private lateinit var config: Config

    @Bean
    open fun processedMessagesCache(readOnlyProcessedMessagesDatabaseAccessor: ReadOnlyProcessedMessagesDatabaseAccessor): ProcessedMessagesCache {
        val messages = readOnlyProcessedMessagesDatabaseAccessor.get()
        return ProcessedMessagesCache(config.me.processedMessagesInterval, messages)
    }
}