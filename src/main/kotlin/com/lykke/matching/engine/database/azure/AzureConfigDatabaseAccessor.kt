package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.Settings
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
        private const val SETTING_STATE_COLUMN_NAME = "Enabled"
        private val DISABLED_ASSETS = "DisabledAssets"
        private val TRUSTED_CLIENTS = "TrustedClients"
    }

    private val configTable: CloudTable = getOrCreateTable(connectionString, configTableName)

    override fun loadConfigs(): Settings? {
        return try {
            val partitionFilter = TableQuery.generateFilterCondition(SETTING_STATE_COLUMN_NAME, TableQuery.QueryComparisons.EQUAL, true)
            val partitionQuery = TableQuery.from(AzureAppProperty::class.java).where(partitionFilter)

            val settings : Map<String, Set<String>> = configTable.execute(partitionQuery)
                    .groupBy { it.name }
                    .mapValues { HashSet(it.value.map { it.value }) }

            toSettings(settings)
        } catch (e: Exception) {
            val message = "Unable to load application configs"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            null
        }
    }

    private fun toSettings(settings : Map<String, Set<String>>?): Settings? {
        if (settings == null) {
            return null
        }

        val trustedClients = settings[TRUSTED_CLIENTS]?.toMutableSet() ?: HashSet()
        val disabledAssets = settings[DISABLED_ASSETS]?.toMutableSet() ?: HashSet()

        return Settings(trustedClients = trustedClients, disabledAssets = disabledAssets)
    }
}