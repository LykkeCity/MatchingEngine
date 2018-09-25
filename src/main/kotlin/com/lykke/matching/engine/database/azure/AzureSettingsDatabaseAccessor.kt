package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.settings.AzureAppSetting
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import org.springframework.util.CollectionUtils

class AzureSettingsDatabaseAccessor(connectionString: String, configTableName: String) : SettingsDatabaseAccessor {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureSettingsDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()

        private const val ROW_KEY = "RowKey"
        private const val PARTITION_KEY = "PartitionKey"

        private const val SETTING_STATE_COLUMN_NAME = "Enabled"

    }

    private val settingsTable: CloudTable = getOrCreateTable(connectionString, configTableName)

    override fun getSetting(settingGroup: AvailableSettingGroup, settingName: String, enabled: Boolean?): Setting? {
        val azureSetting = getAzureSetting(settingGroup, settingName, enabled)
        return azureSetting?.let {
            toSetting(it)
        }
    }

    override fun getSettingsGroup(settingGroup: AvailableSettingGroup, enabled: Boolean?): SettingsGroup? {
        return toSettingsGroup(getAzureSettingsForGroup(settingGroup, enabled))
    }

    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup> {
        return toSettingsGroup(getAllGroupNamesToAzureSettings(enabled))
    }

    override fun deleteSetting(settingGroup: AvailableSettingGroup, settingName: String) {
        getAzureSetting(settingGroup, settingName)?.let {
            settingsTable.execute(TableOperation.delete(it))
        }
    }

    override fun deleteSettingsGroup(settingGroup: AvailableSettingGroup) {
        val settings = getAzureSettingsForGroup(settingGroup)
        batchDelete(settingsTable, settings)
    }

    override fun createOrUpdateSetting(settingGroup: AvailableSettingGroup, setting: Setting) {
        try {
            val azureSetting = toAzureSetting(settingGroup, setting)
            settingsTable.execute(TableOperation.insertOrMerge(azureSetting))
        } catch (e: Exception) {
            throw RuntimeException("Not able persist setting for group: ${settingGroup.settingGroupName}, name: ${setting.name}", e)
        }
    }

    private fun getAllGroupNamesToAzureSettings(enabled: Boolean? = null): Map<String, List<AzureAppSetting>> {
        return try {

            val enabledQuery = enabled?.let {
                val enabledFilter = getEnabledFlagFilter(enabled)
                TableQuery.from(AzureAppSetting::class.java).where(enabledFilter)
            }

            val query = enabledQuery ?: TableQuery.from(AzureAppSetting::class.java)

            return settingsTable.execute(query).groupBy { it.partitionKey }
        } catch (e: Exception) {
            val message = "Unable to load all application setting groups"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            emptyMap()
        }
    }

    private fun getAzureSettingsForGroup(settingGroup: AvailableSettingGroup, enabled: Boolean? = null): List<AzureAppSetting> {
        return try {
            val partitionFilter = getGroupNameSetting(settingGroup)
            val combinedFiler = getCombinedFilterUseLogicalAnd(partitionFilter, getEnabledFlagFilter(enabled))

            val query = TableQuery.from(AzureAppSetting::class.java).where(combinedFiler)

            settingsTable.execute(query).toList()
        } catch (e: Exception) {
            val message = "Unable to load application settings for group: ${settingGroup.settingGroupName}"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            emptyList()
        }
    }

    private fun getAzureSetting(settingGroup: AvailableSettingGroup, settingName: String, enabled: Boolean? = null): AzureAppSetting? {
        return try {
            val partitionFilter = getGroupNameSetting(settingGroup)
            val rowFilter = TableQuery.generateFilterCondition(ROW_KEY, TableQuery.QueryComparisons.EQUAL, settingName)
            val enabledFiler = getEnabledFlagFilter(enabled)

            val query = TableQuery.from(AzureAppSetting::class.java).where(getCombinedFilterUseLogicalAnd(partitionFilter, rowFilter, enabledFiler))

            settingsTable.execute(query)?.firstOrNull()
        } catch (e: Exception) {
            val message = "Unable to load single application setting for group: ${settingGroup.settingGroupName}, setting: $settingName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            null
        }
    }

    private fun toSettingsGroup(settings: List<AzureAppSetting>): SettingsGroup? {
        if (CollectionUtils.isEmpty(settings)) {
            return null
        }
        return SettingsGroup(AvailableSettingGroup.getBySettingsGroupName(settings.first().partitionKey), toSettings(settings))
    }

    private fun toSettingsGroup(groupToSettings: Map<String, List<AzureAppSetting>>): Set<SettingsGroup> {
        return groupToSettings.map { entry -> SettingsGroup(AvailableSettingGroup.getBySettingsGroupName(entry.key), toSettings(entry.value)) }.toSet()
    }

    private fun toSetting(azureSetting: AzureAppSetting): Setting {
        return Setting(azureSetting.rowKey, azureSetting.value, azureSetting.enabled)
    }

    private fun toAzureSetting(settingsGroup: AvailableSettingGroup, setting: Setting): AzureAppSetting {
        return AzureAppSetting(settingsGroup.settingGroupName, setting.name, setting.value, setting.enabled)
    }

    private fun toSettings(azureSettings: List<AzureAppSetting>): Set<Setting> {
        return azureSettings
                .map(::toSetting)
                .toSet()
    }

    private fun getEnabledFlagFilter(enabled: Boolean?): String? {
        return enabled?.let {
            TableQuery.generateFilterCondition(SETTING_STATE_COLUMN_NAME, TableQuery.QueryComparisons.EQUAL, it)
        }
    }

    private fun getGroupNameSetting(settingGroup: AvailableSettingGroup): String {
        return TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, settingGroup.settingGroupName)
    }
}