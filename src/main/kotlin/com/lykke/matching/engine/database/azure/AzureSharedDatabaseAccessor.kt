package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.KeepAliveUpdate
import com.lykke.matching.engine.database.SharedDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import org.apache.log4j.Logger
import java.util.Date

class AzureSharedDatabaseAccessor: SharedDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureSharedDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val monitoringTable: CloudTable

    constructor(sharedConfig: String) {
        this.monitoringTable = getOrCreateTable(sharedConfig, "Monitoring")
    }

    override fun updateKeepAlive(date: Date) {
        try {
            monitoringTable.execute(TableOperation.insertOrMerge(KeepAliveUpdate(date)))
        } catch(e: Exception) {
            LOGGER.error("Unable to update keep alive", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to update keep alive", e)
        }
    }
}