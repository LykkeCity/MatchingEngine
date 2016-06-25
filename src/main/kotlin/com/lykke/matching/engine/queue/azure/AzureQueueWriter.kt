package com.lykke.matching.engine.queue.azure

import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.queue.QueueWriter
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.queue.CloudQueue
import com.microsoft.azure.storage.queue.CloudQueueMessage
import com.sun.javaws.exceptions.InvalidArgumentException
import org.apache.log4j.Logger
import java.util.HashMap

class AzureQueueWriter: QueueWriter {

    companion object {
        val LOGGER = Logger.getLogger(AzureQueueWriter::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val outQueue: CloudQueue

    constructor(queueConnectionString: String, azureConfig: HashMap<String, Any>) {
        val storageAccount = CloudStorageAccount.parse(queueConnectionString)
        val queueClient = storageAccount.createCloudQueueClient()
        var queueName = (azureConfig["MatchingEngine"] as Map<String, Any>).get("BackendQueueName") as String?
        if (queueName == null) {
            queueName = "indata"
        }
        outQueue = queueClient.getQueueReference(queueName)

        // Create the queue if it doesn't already exist.
        if (!outQueue.exists()) {
            throw InvalidArgumentException(arrayOf("Back Office queue does not exists"))
        }
    }

    override fun write(data: String) {
        try {
            outQueue.addMessage(CloudQueueMessage(data))
        } catch (e: Exception) {
            LOGGER.error("Unable to enqueue message to azure queue: $data", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to enqueue message to azure queue: $data", e)
        }
    }
}