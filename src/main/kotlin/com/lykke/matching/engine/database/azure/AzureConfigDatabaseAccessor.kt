package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.config.AzureAppProperty
import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableQuery

class AzureConfigDatabaseAccessor(connectionString: String, configTableName: String = "MatchingEngineSettings") : ConfigDatabaseAccessor  {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureConfigDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val SETTING_STATE_COLUMN_NAME = "Disabled"
    }

    private val configTable: CloudTable = getOrCreateTable(connectionString, configTableName)

    override fun loadConfigs(): Map<String, Set<String>>? {
        return try {
            val partitionFilter = TableQuery.generateFilterCondition(SETTING_STATE_COLUMN_NAME, TableQuery.QueryComparisons.EQUAL, true)
            val partitionQuery = TableQuery.from(AzureAppProperty::class.java).where(partitionFilter)
            configTable.execute(partitionQuery).groupBy { it.name }.mapValues { HashSet(it.value.map { it.value }) }
        } catch (e: Exception) {
            val message = "Unable to load application configs"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            null
        }
    }
}