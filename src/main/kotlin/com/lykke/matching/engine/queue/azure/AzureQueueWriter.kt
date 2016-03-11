package com.lykke.matching.engine.queue.azure

import com.lykke.matching.engine.queue.QueueWriter
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.queue.CloudQueue
import com.microsoft.azure.storage.queue.CloudQueueMessage
import com.sun.javaws.exceptions.InvalidArgumentException
import org.apache.log4j.Logger

class AzureQueueWriter: QueueWriter {

    companion object {
        val LOGGER = Logger.getLogger(AzureQueueWriter::class.java.name)
    }

    private val outQueue: CloudQueue

    constructor(queueConnectionString: String?) {

        val storageAccount = CloudStorageAccount.parse(queueConnectionString)
        val queueClient = storageAccount.createCloudQueueClient()
        outQueue = queueClient.getQueueReference("indata")

        // Create the queue if it doesn't already exist.
        if (!outQueue.exists()) {
            throw InvalidArgumentException(arrayOf("Back Office queue does not exists"))
        }
    }

    override fun write(data: String) {
        try {
            outQueue.addMessage(CloudQueueMessage(data))
        } catch (e: Exception) {
            LOGGER.error("Unable to enqueue message to azure queue: $data")
        }
    }
}