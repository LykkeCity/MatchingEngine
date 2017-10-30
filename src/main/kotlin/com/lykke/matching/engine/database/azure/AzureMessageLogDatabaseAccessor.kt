package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.AzureMessage
import com.lykke.matching.engine.database.MessageLogDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import org.apache.log4j.Logger

class AzureMessageLogDatabaseAccessor(connString: String) : MessageLogDatabaseAccessor<AzureMessage> {

    companion object {
        private val LOGGER = Logger.getLogger(AzureMessageLogDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val eventLogTable: CloudTable = getOrCreateTable(connString, "MatchingEngineMessageLog")

    override fun log(message: AzureMessage) {
        try {
            eventLogTable.execute(TableOperation.insert(message))
        } catch (e: Exception) {
            val errorMessage = "Unable to insert message log: ${e.message}"
            LOGGER.error(errorMessage, e)
            METRICS_LOGGER.logError(errorMessage, e)
        }
    }
}