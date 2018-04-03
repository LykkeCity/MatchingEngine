package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.TickUpdateInterval
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.history.TickBlobHolder
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.blob.CloudBlob
import com.microsoft.azure.storage.blob.CloudBlobContainer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.*
import java.util.stream.Collectors

class AzureHistoryTicksDatabaseAccessor(historyTicksString: String, val frequency: Long) : HistoryTicksDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(AzureHistoryTicksDatabaseAccessor::class.java.name)

        val METRICS_LOGGER = MetricsLogger.getLogger()
        val PRICE_PAIR_DELIMITER = ";"
        val PRICE_DELIMITER = ","
        val BLOB_REFERENCE_PREFIX  = "BA"
        val BLOB_NAME_PATTERN = "BA_%s_%s"
    }

    private val historyBlobContainer: CloudBlobContainer = getOrCreateBlob(historyTicksString, "history")


    override fun loadHistoryTicks(): List<TickBlobHolder> {
         return loadHistoryBlobs()
                 .stream()
                 .map(this::blobToTick)
                 .collect(Collectors.toList())
     }

    override fun loadHistoryTick(asset: String, tickUpdateInterval: TickUpdateInterval): TickBlobHolder? {
        val blob = loadHistoryBlob(asset, tickUpdateInterval) ?: return null
        return blobToTick(blob)
    }

    override fun saveHistoryTick(tick: TickBlobHolder) {
        val blobName = getBlobName(tick.assetPair, tick.tickUpdateInterval)
        try {
            val blob = historyBlobContainer.getBlockBlobReference(blobName)
            val byteArray = tick.toString().toByteArray()

            blob.upload(ByteArrayInputStream(byteArray), byteArray.size.toLong())
        } catch (e: Exception) {
            LOGGER.error("Unable to save blob $blobName", e)
            METRICS_LOGGER.logError( "Unable to save blob $blobName", e)
        }
    }

    private fun loadHistoryBlob(asset: String,tickUpdateInterval: TickUpdateInterval): CloudBlob? {
        try {
            val blobItem = historyBlobContainer.getBlockBlobReference(getBlobName(asset, tickUpdateInterval))
            if (blobItem.exists()) {
                return blobItem
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to load blobs", e)
            METRICS_LOGGER.logError( "Unable to load blobs", e)
        }
        return null
    }

    private fun loadHistoryBlobs(): List<CloudBlob> {
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

    private fun blobToTick(blob: CloudBlob): TickBlobHolder {
        blob.downloadAttributes()
        val names = blob.name.split("_")
        val assetPair = names[1]
        val interval = TickUpdateInterval.valueOf(names[2])

        val askTicks = LinkedList<Double>()
        val bidTicks = LinkedList<Double>()

        val data = blobToString(blob)

        if (data != null) {
            data.split(PRICE_PAIR_DELIMITER).forEach {
                val prices = it.split(PRICE_DELIMITER)
                askTicks.add(prices[0].toDouble())
                bidTicks.add(prices[1].toDouble())
            }
        }

        return TickBlobHolder(assetPair = assetPair,
                tickUpdateInterval =  interval,
                askTicks = askTicks,
                bidTicks = bidTicks,
                lastUpdate = blob.properties.lastModified.time,
                frequency = frequency)
    }

    private fun blobToString(blob: CloudBlob?): String? {
        if (blob != null) {
            val outputStream = ByteArrayOutputStream()
            blob.download(outputStream)
            return outputStream.toString()
        }
        return null
    }

    private fun getBlobName(assetPair: String, interval: TickUpdateInterval): String {
        return BLOB_NAME_PATTERN.format(assetPair, interval.prefix)
    }
}