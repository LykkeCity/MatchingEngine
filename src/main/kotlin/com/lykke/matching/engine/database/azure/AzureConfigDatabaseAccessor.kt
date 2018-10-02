package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.Settings
import com.lykke.matching.engine.daos.azure.config.AzureAppProperty
import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableQuery
import java.math.BigDecimal

class AzureConfigDatabaseAccessor(connectionString: String, configTableName: String) : ConfigDatabaseAccessor  {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureConfigDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
        private const val SETTING_STATE_COLUMN_NAME = "Enabled"
        private const val DISABLED_ASSETS = "DisabledAssets"
        private const val TRUSTED_CLIENTS = "TrustedClients"
        private const val MO_PRICE_DEVIATION_THRESHOLD = "MarketOrderPriceDeviationThreshold"
        private const val LO_PRICE_DEVIATION_THRESHOLD = "LimitOrderPriceDeviationThreshold"

        private class Value(val rowKey: String,
                            val value: String)
    }

    private val configTable: CloudTable = getOrCreateTable(connectionString, configTableName)

    override fun loadConfigs(): Settings? {
        return try {
            val partitionFilter = TableQuery.generateFilterCondition(SETTING_STATE_COLUMN_NAME, TableQuery.QueryComparisons.EQUAL, true)
            val partitionQuery = TableQuery.from(AzureAppProperty::class.java).where(partitionFilter)

            val settings: Map<String, Set<Value>> = configTable.execute(partitionQuery)
                    .groupBy { it.partitionKey }
                    .mapValues { HashSet(it.value.map { Value(it.rowKey, it.value) }) }

            toSettings(settings)
        } catch (e: Exception) {
            val message = "Unable to load application configs"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            null
        }
    }

    private fun toSettings(settings: Map<String, Set<Value>>): Settings {
        val trustedClients = settings[TRUSTED_CLIENTS]?.map { it.value }?.toSet() ?: emptySet()

        val disabledAssets = settings[DISABLED_ASSETS]?.map { it.value }?.toSet() ?: emptySet()

        val moPriceDeviationThresholds = settings[MO_PRICE_DEVIATION_THRESHOLD]
                ?.groupBy { it.rowKey }
                ?.mapValues { BigDecimal(it.value.single().value) }
                ?: emptyMap()

        val loPriceDeviationThresholds = settings[LO_PRICE_DEVIATION_THRESHOLD]
                ?.groupBy { it.rowKey }
                ?.mapValues { BigDecimal(it.value.single().value) }
                ?: emptyMap()

        return Settings(trustedClients,
                disabledAssets,
                moPriceDeviationThresholds,
                loPriceDeviationThresholds)
    }
}