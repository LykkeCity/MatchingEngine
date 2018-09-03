package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.settings.AzureAppSettingHistory
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.database.SettingsHistoryDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery

class AzureSettingsHistoryDatabaseAccessor(connectionString: String, configTableName: String): SettingsHistoryDatabaseAccessor {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureSettingsHistoryDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()

        private const val PARTITION_KEY = "PartitionKey"
        private const val NAME_COLUMN = "SettingName"
    }

    private val historyTable: CloudTable = getOrCreateTable(connectionString, configTableName)

    override fun add(settingGroupName: String, setting: Setting) {
        try {
            historyTable.execute(TableOperation.insertOrMerge(toAzureSettingHistory(settingGroupName, setting)))
        } catch (e: Exception) {
            val message = "Not able to persist setting to the history, group: $settingGroupName, name: ${setting.name}"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }

    override fun get(settingGroupName: String, settingName: String): List<Setting> {
        val partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, settingGroupName)
        val nameFilter =  TableQuery.generateFilterCondition(NAME_COLUMN, TableQuery.QueryComparisons.EQUAL, settingName)

        val query = TableQuery.from(AzureAppSettingHistory::class.java)
                .where(getCombinedFilterUseLogicalAnd(partitionFilter, nameFilter))

        return historyTable.execute(query).map { toSetting(it) }
    }

    private fun toSetting(azureAppSettingHistory: AzureAppSettingHistory): Setting {
        return azureAppSettingHistory.let {
            Setting(it.settingName, it.value, it.enabled, it.comment, it.user, it.timestamp)
        }
    }

    private fun toAzureSettingHistory(settingGroupName: String, setting: Setting): AzureAppSettingHistory {
        return AzureAppSettingHistory(settingGroupName,  setting.name, setting.value, setting.comment, setting.user, setting.enabled)
    }
}