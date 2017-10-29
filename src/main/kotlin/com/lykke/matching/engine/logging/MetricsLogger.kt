package com.lykke.matching.engine.logging

import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.fixedRateTimer

class MetricsLogger {
    companion object {
        val LOGGER = Logger.getLogger(MetricsLogger::class.java.name)!!
        val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")

        val sentTimestamps = ConcurrentHashMap<String, Long>()
        var throttlingLimit: Int = 0

        val ERROR_QUEUE = LinkedBlockingQueue<LoggableObject>()

        fun init(azureQueueConnectionString: String,
                 queueName: String,
                 throttlingLimitSeconds: Int,
                 messagesTtlMinutes: Int = 60,
                 cleanerInterval: Long = 3 * 60 * 60 * 1000 // each 3 hour
        ) {

            AzureQueueLogger(azureQueueConnectionString, queueName, ERROR_QUEUE).start()

            throttlingLimit = throttlingLimitSeconds * 1000

            fixedRateTimer(name = "ErrorLoggerCleaner", initialDelay = cleanerInterval, period = cleanerInterval) {
                clearSentMessageTimestamps(messagesTtlMinutes)
            }
        }

        fun getLogger(): MetricsLogger {
            return MetricsLogger()
        }

        fun clearSentMessageTimestamps(ttlMinutes: Int) {
            var removedItems = 0
            val threshold = Date().time - ttlMinutes * 60 * 1000
            val iterator = sentTimestamps.iterator()
            while (iterator.hasNext()) {
                val entry = iterator.next()
                if (entry.value < threshold) {
                    iterator.remove()
                    removedItems++
                }
            }
            LOGGER.debug("Removed $removedItems from ErrorsLogger")
        }
    }

    fun logError(note: String, exception: Exception? = null) {
        if (!messageWasSentWithinTimeout(note)) {
            ERROR_QUEUE.put(Error("Errors", "${LocalDateTime.now().format(DATE_TIME_FORMATTER)}: $note ${exception?.message ?: ""}"))
            sentTimestamps[note] = Date().time
        }
    }

    private fun messageWasSentWithinTimeout(errorMessage: String): Boolean {
        val lastSentTimestamp = sentTimestamps[errorMessage]
        return lastSentTimestamp != null && lastSentTimestamp > Date().time - throttlingLimit
    }
}