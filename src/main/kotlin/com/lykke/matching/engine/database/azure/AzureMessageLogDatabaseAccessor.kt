package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.Message
import com.lykke.matching.engine.daos.azure.AzureMessage
import com.lykke.matching.engine.database.MessageLogDatabaseAccessor
import com.lykke.utils.MAX_AZURE_FIELD_LENGTH
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.lykke.utils.string.parts
import com.microsoft.azure.storage.blob.CloudBlobContainer
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableServiceException
import java.io.ByteArrayInputStream
import java.net.HttpURLConnection
import java.util.UUID

class AzureMessageLogDatabaseAccessor(connString: String, tableName: String) : MessageLogDatabaseAccessor<Message> {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureMessageLogDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val eventLogTable: CloudTable = getOrCreateTable(connString, tableName)
    private val logsBlobContainer: CloudBlobContainer = getOrCreateBlob(connString, "me-messages")

    override fun log(message: Message) {
        try {
            var azureMessage = AzureMessage(message.id, message.timestamp, parts(message.message))
            var counter = 0
            var blobName: String? = null
            val rowKey = azureMessage.rowKey
            while (true) {
                try {
                    if (counter > 0) {
                        azureMessage.rowKey = String.format("%s_%03d", rowKey, counter)
                    }
                    eventLogTable.execute(TableOperation.insert(azureMessage))
                    return
                } catch (e: TableServiceException) {
                    if (blobName == null && (e.httpStatusCode == HttpURLConnection.HTTP_BAD_REQUEST || e.httpStatusCode == HttpURLConnection.HTTP_ENTITY_TOO_LARGE)) {
                        blobName = saveToBlob(message.message)
                        azureMessage = AzureMessage(message.id, message.timestamp, blobName)
                    } else if (e.httpStatusCode == HttpURLConnection.HTTP_CONFLICT && counter < 999) {
                        counter++
                    } else {
                        throw e
                    }
                }
            }
        } catch (e: Exception) {
            val errorMessage = "Unable to insert message log (id: ${message.id}, type: ${message.type}): ${e.message}"
            LOGGER.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }

    private fun saveToBlob(message: String): String {
        val blobName = UUID.randomUUID().toString()
        val byteArray = message.toByteArray()
        logsBlobContainer.getBlockBlobReference(blobName).upload(ByteArrayInputStream(byteArray), byteArray.size.toLong())
        return blobName
    }

    private fun parts(value: String): Array<String> = value.parts(MAX_AZURE_FIELD_LENGTH, 6)
}