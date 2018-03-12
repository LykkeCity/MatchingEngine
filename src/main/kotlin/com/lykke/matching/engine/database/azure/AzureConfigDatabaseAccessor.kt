package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.config.AzureAppProperty
import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery

class AzureConfigDatabaseAccessor(connectionString: String, configTableName: String = "MatchingEngineSettings") : ConfigDatabaseAccessor {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureConfigDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()

        private const val PARTITION_KEY = "MatchingEngineProperty"
    }

    private val configTable: CloudTable = getOrCreateTable(connectionString, configTableName)

    override fun loadConfigs(): Map<String, String>? {
        return try {
            val partitionFilter = TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, PARTITION_KEY)
            val partitionQuery = TableQuery.from(AzureAppProperty::class.java).where(partitionFilter)
            configTable.execute(partitionQuery).groupBy { it.name }.mapValues { it.value.first().value }
        } catch (e: Exception) {
            val message = "Unable to load application configs"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            null
        }
    }

    override fun saveValue(name: String, value: String) {
        try {
            configTable.execute(TableOperation.insertOrMerge(AzureAppProperty(PARTITION_KEY, name, value)))
        } catch (e: Exception) {
            val message = "Unable to save value: $value, name: $name"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }
}