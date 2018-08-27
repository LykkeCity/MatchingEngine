package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.settings.AzureAppSetting
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
        private const val DISABLED_ASSETS = "DisabledAssets"
        private const val TRUSTED_CLIENTS = "TrustedClients"
        private const val MO_PRICE_DEVIATION_THRESHOLD = "MarketOrderPriceDeviationThreshold"
        private const val LO_PRICE_DEVIATION_THRESHOLD = "LimitOrderPriceDeviationThreshold"
    }

    private val settingsTable: CloudTable = getOrCreateTable(connectionString, configTableName)

    override fun getSetting(settingGroupName: String, settingName: String, enabled: Boolean?): Setting? {
        val azureSetting = getAzureSetting(settingGroupName, settingName, enabled)
        return azureSetting?.let {
            toSetting(it)
        }
    }

    override fun getSettingsGroup(settingGroupName: String, enabled: Boolean?): SettingsGroup? {
        return toSettingsGroup(getAzureSettingsForGroup(settingGroupName, enabled))
    }

    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup> {
        return toSettingsGroup(getAllGroupNamesToAzureSettings(enabled))
    }

    override fun deleteSetting(settingGroupName: String, settingName: String) {
        getAzureSetting(settingGroupName, settingName)?.let {
            settingsTable.execute(TableOperation.delete(it))
        }
    }

    override fun deleteSettingsGroup(settingGroupName: String) {
        val settings = getAzureSettingsForGroup(settingGroupName)
        batchDelete(settingsTable, settings)
    }

    override fun createOrUpdateSetting(settingGroupName: String, setting: Setting) {
        val azureSetting = toAzureSetting(settingGroupName, setting)
        settingsTable.execute(TableOperation.insertOrMerge(azureSetting))
    }

    private fun getAllGroupNamesToAzureSettings(enabled: Boolean? = null): Map<String, List<AzureAppSetting>> {
        return try {

            val enabledQuery = enabled?.let {
                val enabledFilter = getEnabledFlagFilter(enabled)
                TableQuery.from(AzureAppSetting::class.java).where(enabledFilter)
            }

            val query = enabledQuery ?: TableQuery()

            return settingsTable.execute(query).groupBy { it.partitionKey }
        } catch (e: Exception) {
            val message = "Unable to load all application setting groups"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            emptyMap()
        }
    }

    private fun getAzureSettingsForGroup(settingGroupName: String, enabled: Boolean? = null): List<AzureAppSetting> {
        return try {
            val partitionFilter = getGroupNameSetting(settingGroupName)
            val combinedFiler = getCombinedFilterUseLogicalAnd(partitionFilter, getEnabledFlagFilter(enabled))

            val partitionQuery = TableQuery.from(AzureAppSetting::class.java).where(combinedFiler)

            settingsTable.execute(partitionQuery).toList()
        } catch (e: Exception) {
            val message = "Unable to load application settings for group: $settingGroupName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            emptyList()
        }
    }

    private fun getAzureSetting(settingGroupName: String, settingName: String, enabled: Boolean? = null): AzureAppSetting? {
        return try {
            val partitionFilter = getGroupNameSetting(settingGroupName)
            val rowFilter = TableQuery.generateFilterCondition(ROW_KEY, TableQuery.QueryComparisons.EQUAL, settingName)
            val enabledFiler = getEnabledFlagFilter(enabled)

            val query = TableQuery.from(AzureAppSetting::class.java).where(getCombinedFilterUseLogicalAnd(partitionFilter, rowFilter, enabledFiler))

            settingsTable.execute(query)?.first()
        } catch (e: Exception) {
            val message = "Unable to load single application setting for group: $settingGroupName, setting: $settingName"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            null
        }
    }

    private fun toSettingsGroup(settings: List<AzureAppSetting>): SettingsGroup? {
        if (CollectionUtils.isEmpty(settings)) {
            return null
        }
        return SettingsGroup(settings.first().partitionKey, toSettings(settings))
    }

    private fun toSettingsGroup(groupToSettings: Map<String, List<AzureAppSetting>>): Set<SettingsGroup> {
        return groupToSettings.map { entry -> SettingsGroup(entry.key, toSettings(entry.value)) }.toSet()
    }

    private fun toSetting(azureSetting: AzureAppSetting): Setting {
        return Setting(azureSetting.rowKey, azureSetting.value, azureSetting.isEnabled, azureSetting.comment)
    }

    private fun toAzureSetting(settingsGroupName: String, setting: Setting): AzureAppSetting {
        return AzureAppSetting(settingsGroupName, setting.name, setting.value, setting.comment)
    }

    private fun toSettings(azureSettings: List<AzureAppSetting>): Set<Setting> {
        return azureSettings
                .map { Setting(it.rowKey, it.value, it.isEnabled, it.comment) }
                .toSet()
    }

    private fun getEnabledFlagFilter(enabled: Boolean?): String? {
        return enabled?.let {
            TableQuery.generateFilterCondition(SETTING_STATE_COLUMN_NAME, TableQuery.QueryComparisons.EQUAL, it)
        }
    }

    private fun getGroupNameSetting(settingGroupName: String): String {
        return TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, settingGroupName)
    }
}