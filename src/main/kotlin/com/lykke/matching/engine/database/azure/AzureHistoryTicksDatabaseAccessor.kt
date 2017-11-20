package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.history.TickBlobHolder
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.microsoft.azure.storage.blob.CloudBlob
import com.microsoft.azure.storage.blob.CloudBlobContainer
import java.io.ByteArrayInputStream
import java.util.LinkedList

class AzureHistoryTicksDatabaseAccessor(historyTicksString: String) : HistoryTicksDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(AzureHistoryTicksDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val historyBlobContainer: CloudBlobContainer = getOrCreateBlob(historyTicksString, "history")

    override fun loadHistoryTick(asset: String, period: String): CloudBlob? {
        try {
            val blobItem = historyBlobContainer.getBlockBlobReference("BA_${asset}_$period")
            if (blobItem.exists()) {
                return blobItem
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to load blobs", e)
            METRICS_LOGGER.logError( "Unable to load blobs", e)
        }
        return null
    }

    override fun loadHistoryTicks(): List<CloudBlob> {
        val result = LinkedList<CloudBlob>()
        try {
            // Loop over blobs within the container and output the URI to each of them.
            historyBlobContainer.listBlobs()
                    .filterIsInstance<CloudBlob>()
                    .filterTo(result) { it.name.startsWith("BA_") }
        } catch (e: Exception) {
            LOGGER.error("Unable to load blobs", e)
            METRICS_LOGGER.logError( "Unable to load blobs", e)
        }
        return result
    }

    override fun saveHistoryTick(tick: TickBlobHolder) {
        try {
            val blob = historyBlobContainer.getBlockBlobReference(tick.name)
            val byteArray = tick.convertToString().toByteArray()
            blob.upload(ByteArrayInputStream(byteArray), byteArray.size.toLong())
        } catch (e: Exception) {
            LOGGER.error("Unable to save blob ${tick.name}", e)
            METRICS_LOGGER.logError( "Unable to save blob ${tick.name}", e)
        }
    }
}