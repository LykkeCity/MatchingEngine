package com.lykke.matching.engine.deduplication

import com.lykke.matching.engine.database.ReadOnlyProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.TaskScheduler
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.annotation.PostConstruct
import kotlin.collections.HashMap
import kotlin.concurrent.read
import kotlin.concurrent.write

@Component
class ProcessedMessagesCache @Autowired constructor(
        private val readOnlyProcessedMessagesDatabaseAccessor: ReadOnlyProcessedMessagesDatabaseAccessor,
        private val config: Config,
        private val taskScheduler: TaskScheduler) {

    private var typeToProcessedMessage = HashMap<Byte, MutableSet<ProcessedMessage>>()
    private var prevTypeToProcessedMessage = HashMap<Byte, MutableSet<ProcessedMessage>>()
    private val lock = ReentrantReadWriteLock()

    fun addMessage(message: ProcessedMessage) {
        if (ProcessedMessageUtils.isDeduplicationNotNeeded(message.type)) {
            return
        }
        lock.write {
            typeToProcessedMessage.getOrPut(message.type) { HashSet() }.add(message)
        }
    }

    fun isProcessed(type: Byte, messageId: String): Boolean {
        if (ProcessedMessageUtils.isDeduplicationNotNeeded(type)) {
            return false
        }

        lock.read {
            return containsMessageId(typeToProcessedMessage[type], messageId)
                    || containsMessageId(prevTypeToProcessedMessage[type], messageId)
        }
    }

    private fun containsMessageId(processedMessages: Set<ProcessedMessage>?, messageId: String): Boolean {
        if (processedMessages == null) {
            return false
        }

        return processedMessages.find { it.messageId == messageId } != null
    }

    @PostConstruct
    private fun initCache() {
        val cutoffTime = getCutoffTime()
        readOnlyProcessedMessagesDatabaseAccessor.get().forEach {
            if (it.timestamp > cutoffTime) {
                typeToProcessedMessage.getOrPut(it.type) { HashSet() }.add(it)
            }
        }

        scheduleCleaning()
    }

    private fun clean() {
        lock.write {
            prevTypeToProcessedMessage = typeToProcessedMessage
            typeToProcessedMessage = HashMap()
        }
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