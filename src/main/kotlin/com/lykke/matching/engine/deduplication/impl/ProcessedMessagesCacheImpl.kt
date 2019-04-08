package com.lykke.matching.engine.deduplication.impl

import com.lykke.matching.engine.database.ReadOnlyProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.scheduling.TaskScheduler
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.PostConstruct
import kotlin.collections.HashMap

@Component
class ProcessedMessagesCacheImpl @Autowired constructor(
        @Qualifier("MultiSourceProcessedMessageDatabaseAccessor")
        private val readOnlyProcessedMessagesDatabaseAccessor: ReadOnlyProcessedMessagesDatabaseAccessor,
        private val config: Config,
        private val taskScheduler: TaskScheduler) : ProcessedMessagesCache {

    private var typeToProcessedMessage = AtomicReference<HashMap<Byte, MutableSet<String>>>()
    private var prevTypeToProcessedMessage =  AtomicReference<HashMap<Byte, MutableSet<String>>>()

    override fun addMessage(message: ProcessedMessage) {
        typeToProcessedMessage.get().getOrPut(message.type) { HashSet() }.add(message.messageId)
    }

    override fun isProcessed(type: Byte, messageId: String): Boolean {
        return containsMessageId(typeToProcessedMessage.get()[type], messageId)
                || containsMessageId(prevTypeToProcessedMessage.get()[type], messageId)
    }

    private fun containsMessageId(processedMessages: Set<String>?, messageId: String): Boolean {
        if (processedMessages == null) {
            return false
        }

        return processedMessages.contains(messageId)
    }

    @PostConstruct
    private fun initCache() {
        typeToProcessedMessage.set(HashMap())
        prevTypeToProcessedMessage.set(HashMap())
        val cutoffTime = getCutoffTime()
        readOnlyProcessedMessagesDatabaseAccessor.get().forEach {
            if (it.timestamp > cutoffTime) {
                typeToProcessedMessage.get().getOrPut(it.type) { HashSet() }.add(it.messageId)
            }
        }

        scheduleCleaning()
    }

    private fun clean() {
        prevTypeToProcessedMessage.set(typeToProcessedMessage.get())
        typeToProcessedMessage.set(HashMap())
    }

    private fun scheduleCleaning() {
        taskScheduler.scheduleAtFixedRate(
                {
                    Thread.currentThread().name = "ProcessedMessagesCacheCleaner"
                    clean()
                },
                getStarCleaningTime(),
                Duration.ofMillis(config.me.processedMessagesInterval))
    }

    private fun getStarCleaningTime(): Instant {
        return ZonedDateTime.now().toInstant().plusMillis(config.me.processedMessagesInterval)
    }

    private fun getCutoffTime(): Long {
        return Date().time - config.me.processedMessagesInterval
    }
}