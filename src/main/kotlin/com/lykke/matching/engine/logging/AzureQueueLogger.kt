package com.lykke.matching.engine.logging

import com.lykke.matching.engine.queue.QueueWriter
import com.lykke.matching.engine.queue.azure.AzureQueueWriter
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class AzureQueueLogger(queueConnectionString: String, queueName: String, val queue: BlockingQueue<LoggableObject>) : Thread() {
    companion object {
        val LOGGER = Logger.getLogger(AzureQueueLogger::class.java.name)
    }

    private val queueWriter: QueueWriter

    override fun run() {
        while (true) {
            val obj = queue.take()
            queueWriter.write(obj.getJson())
        }
    }

    init {
        this.queueWriter = AzureQueueWriter(queueConnectionString, queueName)
    }
}