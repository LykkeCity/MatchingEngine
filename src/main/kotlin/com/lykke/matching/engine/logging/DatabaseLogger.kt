package com.lykke.matching.engine.logging

import com.lykke.matching.engine.database.MessageLogDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue
import kotlin.concurrent.thread

class DatabaseLogger<in T>(private val dbAccessor: MessageLogDatabaseAccessor,
                           private val queue: BlockingQueue<MessageWrapper>) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(DatabaseLogger::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    fun log(item: T, stringRepresentation: String) {
        queue.put(MessageWrapper(item as Any, stringRepresentation))
    }

    private fun takeAndSaveMessage() {
        try {
            val message = queue.take()
            dbAccessor.log(toLogMessage(message.item, message.stringRepresentation))
        } catch (e: Exception) {
            val errorMessage = "Unable to write log to DB: ${e.message}"
            LOGGER.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }

    init {
        thread(name = DatabaseLogger::class.java.name) {
            while (true) {
                takeAndSaveMessage()
            }
        }
    }
}

class MessageWrapper(val item: Any, val stringRepresentation: String)