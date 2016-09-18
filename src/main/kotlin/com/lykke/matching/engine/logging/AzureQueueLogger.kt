package com.lykke.matching.engine.logging

import com.lykke.matching.engine.queue.QueueWriter
import com.lykke.matching.engine.queue.azure.AzureQueueWriter
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class AzureQueueLogger: Thread {
    companion object {
        val LOGGER = Logger.getLogger(AzureQueueLogger::class.java.name)
    }

    val queueWriter: QueueWriter
    val queue: BlockingQueue<LoggableObject>

    constructor(queueConnectionString: String, queueName: String, queue: BlockingQueue<LoggableObject>) : super() {
        this.queueWriter = AzureQueueWriter(queueConnectionString, queueName)
        this.queue = queue
    }

    override fun run() {
        while (true) {
            val obj = queue.take()
            queueWriter.write(obj.getJson())
        }
    }
}