package com.lykke.matching.engine.logging

import com.lykke.matching.engine.database.MessageLogDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

abstract class DatabaseLogger<in AppFormat, out DbFormat>(private val dbAccessor: MessageLogDatabaseAccessor<DbFormat>) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(DatabaseLogger::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val queue: BlockingQueue<AppFormat> = LinkedBlockingQueue()

    fun log(message: AppFormat) {
        queue.put(message)
    }

    private fun takeAndSaveMessage() {
        try {
            val event = queue.take()
            dbAccessor.log(transformMessage(event))
        } catch (e: Exception) {
            val errorMessage = "Unable to write log to DB: ${e.message}"
            LOGGER.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }

    protected abstract fun transformMessage(message: AppFormat): DbFormat

    init {
        thread(name = DatabaseLogger::class.java.name) {
            while (true) {
                takeAndSaveMessage()
            }
        }
    }
}