package com.lykke.matching.engine.logging

import com.lykke.matching.engine.queue.azure.AzureQueueWriter
import org.apache.log4j.Logger
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.fixedRateTimer

class MetricsLogger {
    companion object {
        private val LOGGER = Logger.getLogger(MetricsLogger::class.java.name)!!
        private val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")

        private val sentTimestamps = ConcurrentHashMap<String, Long>()
        private var throttlingLimit: Int = 0

        private val ERROR_QUEUE = LinkedBlockingQueue<LoggableObject>()

        private const val TYPE_ERROR = "Errors"
        private const val TYPE_WARNING = "Warnings"
        private lateinit var azureQueueConnectionString: String
        private lateinit var queueName: String

        fun init(azureQueueConnectionString: String,
                 queueName: String,
                 throttlingLimitSeconds: Int,
                 messagesTtlMinutes: Int = 60,
                 cleanerInterval: Long = 3 * 60 * 60 * 1000 // each 3 hour
        ) {
            this.azureQueueConnectionString = azureQueueConnectionString
            this.queueName = queueName
            AzureQueueLogger(azureQueueConnectionString, queueName, ERROR_QUEUE).start()

            throttlingLimit = throttlingLimitSeconds * 1000

            fixedRateTimer(name = "ErrorLoggerCleaner", initialDelay = cleanerInterval, period = cleanerInterval) {
                clearSentMessageTimestamps(messagesTtlMinutes)
            }
        }

        fun getLogger(): MetricsLogger {
            return MetricsLogger()
        }

        private fun clearSentMessageTimestamps(ttlMinutes: Int) {
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

        /** Saves 'Warnings' msg directly to azure queue without common thread using */
        fun logWarning(message: String) {
            log(TYPE_WARNING, message)
        }

        private fun log(type: String, message: String) {
            val error = Error(type, "${LocalDateTime.now().format(DATE_TIME_FORMATTER)}: $message")
            AzureQueueWriter(azureQueueConnectionString, queueName).write(error.getJson())
        }
    }

    fun logError(message: String, exception: Exception? = null) {
        log(TYPE_ERROR, message, exception)
    }

    fun logWarning(message: String, exception: Exception? = null) {
        log(TYPE_WARNING, message, exception)
    }

    private fun log(type: String, message: String, exception: Exception? = null) {
        if (!messageWasSentWithinTimeout(type, message)) {
            ERROR_QUEUE.put(Error(type, "${LocalDateTime.now().format(DATE_TIME_FORMATTER)}: $message ${exception?.message ?: ""}"))
            sentTimestamps["$type-$message"] = Date().time
        }
    }

    private fun messageWasSentWithinTimeout(type: String, message: String): Boolean {
        val lastSentTimestamp = sentTimestamps["$type-$message"]
        return lastSentTimestamp != null && lastSentTimestamp > Date().time - throttlingLimit
    }
}