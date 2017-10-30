package com.lykke.matching.engine.logging

import com.lykke.matching.engine.database.MessageLogDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

abstract class DatabaseLogger<out T>(private val dbAccessor: MessageLogDatabaseAccessor<T>) {

    companion object {
        private val LOGGER = Logger.getLogger(DatabaseLogger::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val queue: BlockingQueue<JsonSerializable> = LinkedBlockingQueue()

    fun log(message: JsonSerializable) {
        queue.put(message)
    }

    private fun takeAndSaveMessage() {
        try {
            val event = queue.take()
            transformMessage(event)?.let {
                dbAccessor.log(it)
            }
        } catch (e: Exception) {
            val errorMessage = "Unable to write log to DB: ${e.message}"
            LOGGER.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }

    protected abstract fun transformMessage(message: JsonSerializable): T?

    init {
        Thread {
            while (true) {
                takeAndSaveMessage()
            }
        }.start()
    }
}