package com.lykke.matching.engine.logging

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.LinkedBlockingQueue

class MetricsLogger {
    companion object {
        val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")

        val KEY_VALUE_QUEUE = LinkedBlockingQueue<LoggableObject>()
        val LINE_QUEUE = LinkedBlockingQueue<LoggableObject>()

        var KEY_VALUE_LINK: String? = null
        var LINE_LINK: String? = null

        fun init(keyValueLink: String, lineLink: String) {
            this.KEY_VALUE_LINK = keyValueLink
            this.LINE_LINK = lineLink

            HttpLogger(keyValueLink, KEY_VALUE_QUEUE).start()
            HttpLogger(lineLink, LINE_QUEUE).start()
        }

        fun getLogger(): MetricsLogger {
            return MetricsLogger()
        }
    }

    fun log(keyValue: KeyValue) {
        KEY_VALUE_QUEUE.put(keyValue)
    }

    fun log(line: Line) {
        LINE_QUEUE.put(line)
    }

    fun logError(service: String, note: String, exception: Exception? = null) {
        log(Line(ME_ERRORS, arrayOf(
                KeyValue(SERVICE, service),
                KeyValue(TIMESTAMP, LocalDateTime.now().format(MetricsLogger.DATE_TIME_FORMATTER)),
                KeyValue(NOTE, note),
                KeyValue(STACK_TRACE, if (exception != null) exception.stackTrace.joinToString { "\n" } else "")
        )))
    }
}