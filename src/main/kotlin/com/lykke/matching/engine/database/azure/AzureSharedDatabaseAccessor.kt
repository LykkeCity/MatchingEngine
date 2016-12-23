package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.AzureKeepAliveUpdate
import com.lykke.matching.engine.database.SharedDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import org.apache.log4j.Logger
import java.util.Date

class AzureSharedDatabaseAccessor(sharedConfig: String) : SharedDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureSharedDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val monitoringTable: CloudTable

    override fun updateKeepAlive(date: Date, version: String?) {
        try {
            monitoringTable.execute(TableOperation.insertOrMerge(AzureKeepAliveUpdate(date, version)))
        } catch(e: Exception) {
            LOGGER.error("Unable to update keep alive", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to update keep alive", e)
        }
    }

    init {
        this.monitoringTable = getOrCreateTable(sharedConfig, "Monitoring")
    }
}