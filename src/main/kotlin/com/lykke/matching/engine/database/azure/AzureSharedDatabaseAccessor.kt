package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.AzureKeepAliveUpdate
import com.lykke.matching.engine.daos.azure.AzureKeepAliveUpdate.MATCHING_ENGINE
import com.lykke.matching.engine.daos.azure.AzureKeepAliveUpdate.MONITORING
import com.lykke.matching.engine.database.SharedDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.StorageException
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableRequestOptions
import org.apache.log4j.Logger
import java.util.Date

class AzureSharedDatabaseAccessor(val sharedConfig: String) : SharedDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureSharedDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var monitoringTable = getOrCreateTable(sharedConfig, "Monitoring")

    override fun getLastKeepAlive(): Date? {
        try {
            val query = TableOperation.retrieve(MONITORING, MATCHING_ENGINE, AzureKeepAliveUpdate::class.java)

            val keepAlive = monitoringTable.execute(query).getResultAsType<AzureKeepAliveUpdate>()
            if (keepAlive != null) {
                return keepAlive.dateTime
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load hour candles", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load hour candles", e)
        }

        return null
    }

    override fun updateKeepAlive(date: Date, note: String?) {
        while (true) {
            try {
                val options = TableRequestOptions()
                options.maximumExecutionTimeInMs = 10 * 1000
                monitoringTable.execute(TableOperation.insertOrMerge(AzureKeepAliveUpdate(date, note)), options, null)
                return
            } catch(e: StorageException) {
                val errorCode = e.errorCode
                if ("OperationTimedOut" == errorCode) {
                    LOGGER.error("Unable to update keep alive due to timeout.")
                } else {
                    LOGGER.error("Unable to update keep alive: ${e.errorCode}, ${e.extendedErrorInformation}", e)
                }
                monitoringTable = getOrCreateTable(sharedConfig, "Monitoring")
            } catch(e: Exception) {
                LOGGER.error("Unable to update keep alive", e)
                return
            }
        }
    }
}