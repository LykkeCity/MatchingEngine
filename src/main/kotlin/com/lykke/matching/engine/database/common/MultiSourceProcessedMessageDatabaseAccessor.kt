package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.database.ReadOnlyProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*

@Component("MultiSourceProcessedMessageDatabaseAccessor")
class MultiSourceProcessedMessageDatabaseAccessor
@Autowired constructor(private val redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
                       private val fileProcessedMessagesDatabaseAccessor: FileProcessedMessagesDatabaseAccessor): ReadOnlyProcessedMessagesDatabaseAccessor {
    override fun get(): Set<ProcessedMessage> {
        val result = HashSet<ProcessedMessage>()
        redisProcessedMessagesDatabaseAccessor.ifPresent({ result.addAll(it.get()) })
        result.addAll(fileProcessedMessagesDatabaseAccessor.get())

        return result
    }
}