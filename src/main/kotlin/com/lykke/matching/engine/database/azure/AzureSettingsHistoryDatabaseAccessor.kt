package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.settings.AzureAppSettingHistory
import com.lykke.matching.engine.daos.setting.SettingHistoryRecord
import com.lykke.matching.engine.database.SettingsHistoryDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery

class AzureSettingsHistoryDatabaseAccessor(connectionString: String, configTableName: String) : SettingsHistoryDatabaseAccessor {
    companion object {
        private const val PARTITION_KEY = "PartitionKey"
        private const val NAME_COLUMN = "SettingName"
    }

    private val historyTable: CloudTable = getOrCreateTable(connectionString, configTableName)

    override fun save(settingGroupName: String, settingHistoryRecord: SettingHistoryRecord) {
        try {
            historyTable.execute(TableOperation.insertOrMerge(toAzureSettingHistoryRecord(settingGroupName, settingHistoryRecord)))
        } catch (e: Exception) {
            throw RuntimeException("Not able to persist setting to the history, group: $settingGroupName, name: ${settingHistoryRecord.name}")
        }
    }

    override fun get(settingGroupName: String, settingName: String): List<SettingHistoryRecord> {
        try {
            val partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, settingGroupName)
            val nameFilter = TableQuery.generateFilterCondition(NAME_COLUMN, TableQuery.QueryComparisons.EQUAL, settingName)

            val query = TableQuery.from(AzureAppSettingHistory::class.java)
                    .where(getCombinedFilterUseLogicalAnd(partitionFilter, nameFilter))

            return historyTable.execute(query).map { toSettingHistoryRecord(it) }
        } catch (e: Exception) {
            throw RuntimeException("Not able to get from db history record for: $settingGroupName, setting name: $settingName")
        }
    }

    private fun toSettingHistoryRecord(azureAppSettingHistory: AzureAppSettingHistory): SettingHistoryRecord {
        return azureAppSettingHistory.let {
            SettingHistoryRecord(it.settingName, it.value, it.enabled, it.comment, it.user, it.timestamp)
        }
    }

    private fun toAzureSettingHistoryRecord(settingGroupName: String, settingHistoryRecord: SettingHistoryRecord): AzureAppSettingHistory {
        return AzureAppSettingHistory(settingGroupName,
                settingHistoryRecord.name,
                settingHistoryRecord.value,
                settingHistoryRecord.comment,
                settingHistoryRecord.user,
                settingHistoryRecord.enabled)
    }
}