package com.lykke.matching.engine.logging

import com.lykke.matching.engine.daos.Message
import com.lykke.matching.engine.database.MessageLogDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

class DatabaseLogger(private val dbAccessor: MessageLogDatabaseAccessor) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(DatabaseLogger::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val queue: BlockingQueue<Message> = LinkedBlockingQueue()

    fun log(message: Message) {
        queue.put(message)
    }

    private fun takeAndSaveMessage() {
        try {
            dbAccessor.log(queue.take())
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