package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.settings.DisabledAsset
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableQuery
import org.springframework.stereotype.Component
import java.util.HashSet

@Component
class AzureSettingsDatabaseAccessor(config: Config): SettingsDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(AzureSettingsDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val settingsTable: CloudTable = getOrCreateTable(config.me.db.matchingEngineConnString, "MatchingEngineSettings")

    override fun loadDisabledPairs(): Set<String> {
        val result = HashSet<String>()

        try {
            val partitionQuery = TableQuery.from(DisabledAsset::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, DisabledAsset.DISABLED_ASSET))

            settingsTable.execute(partitionQuery)
                    .filter { it.disabled }
                    .mapTo(result) { it.asset }
        } catch(e: Exception) {
            LOGGER.error("Unable to load disabled assets", e)
        }

        return result
    }
}