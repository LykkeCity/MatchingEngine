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

        val KEY_VALUE_QUEUE = LinkedBlockingQueue<LoggableObject>()
        val LINE_QUEUE = LinkedBlockingQueue<LoggableObject>()

        val sentTimestamps = ConcurrentHashMap<String, Long>()
        var throttlingLimit: Int = 0

        val ERROR_QUEUE = LinkedBlockingQueue<LoggableObject>()

        var KEY_VALUE_LINK: String? = null
        var LINE_LINK: String? = null

        fun init(keyValueLink: String,
                 lineLink: String,
                 azureQueueConnectionString: String,
                 queueName: String,
                 httpLoggerSize: Int,
                 throttlingLimitSeconds: Int,
                 messagesTtlMinutes: Int = 60,
                 cleanerInterval: Long = 3 * 60 * 60 * 1000 // each 3 hour
        ) {
            this.KEY_VALUE_LINK = keyValueLink
            this.LINE_LINK = lineLink

            for (i in 1..httpLoggerSize) {
                HttpLogger(keyValueLink, KEY_VALUE_QUEUE).start()
                HttpLogger(lineLink, LINE_QUEUE).start()
            }

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

    fun log(keyValue: KeyValue) {
        KEY_VALUE_QUEUE.put(keyValue)
    }

    fun log(line: Line) {
        LINE_QUEUE.put(line)
    }

    fun logError(service: String, note: String, exception: Exception? = null, putToErrorQueue: Boolean = true) {
        log(Line(ME_ERRORS, arrayOf(
                KeyValue(SERVICE, service),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(NOTE, note),
                KeyValue(STACK_TRACE, exception?.message ?: "")
        )))

        if (putToErrorQueue) {
            if (!messageWasSentWithinTimeout(note)) {
                ERROR_QUEUE.put(Error("Errors", "${LocalDateTime.now().format(DATE_TIME_FORMATTER)}: $note ${exception?.message ?: ""}"))
                sentTimestamps[note] = Date().time
            }
        }
    }

    private fun messageWasSentWithinTimeout(errorMessage: String): Boolean {
        val lastSentTimestamp = sentTimestamps[errorMessage]
        return lastSentTimestamp != null && lastSentTimestamp > Date().time - throttlingLimit
    }
}