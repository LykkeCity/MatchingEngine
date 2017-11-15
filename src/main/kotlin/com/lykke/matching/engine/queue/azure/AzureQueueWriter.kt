package com.lykke.matching.engine.queue.azure

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.lykke.matching.engine.queue.QueueWriter
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.queue.CloudQueue
import com.microsoft.azure.storage.queue.CloudQueueMessage

class AzureQueueWriter(queueConnectionString: String, queueName: String) : QueueWriter {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(AzureQueueWriter::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val outQueue: CloudQueue

    override fun write(data: String) {
        try {
            outQueue.addMessage(CloudQueueMessage(data))
        } catch (e: Exception) {
            LOGGER.error("Unable to enqueue message to azure queue: $data", e)
            METRICS_LOGGER.logError( "Unable to enqueue message to azure queue: $data", e)
        }
    }

    init {
        val storageAccount = CloudStorageAccount.parse(queueConnectionString)
        val queueClient = storageAccount.createCloudQueueClient()
        outQueue = queueClient.getQueueReference(queueName)
        if (!outQueue.exists()) {
            throw Exception("Azure $queueName queue does not exists")
        }
    }
}