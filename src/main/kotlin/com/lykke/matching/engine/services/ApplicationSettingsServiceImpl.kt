package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.azure.AzureSettingsDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class ApplicationSettingsServiceImpl : ApplicationSettingsService {

    @Autowired
    private lateinit var azureSettingsDatabaseAccessor: AzureSettingsDatabaseAccessor

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    private val DELETE_SETTING_FROM_CACHE_STRATEGIES = mapOf("" to { settingName: String -> applicationSettingsCache.deleteTrustedClient() },
            "" to { name: String -> applicationSettingsCache.deleteTrustedClient(name) })
    private val DELETE_SETTING_GROUP_FROM_CACHE_STRATEGIES = mapOf("" to {settingGroupName: String -> applicationSettingsCache.deleteTrustedClient()})


    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroupDto> {
        return azureSettingsDatabaseAccessor.getAllSettingGroups(enabled).map { toSettingGroupDto(it) }.toSet()
    }

    override fun getSettingsGroup(settingsGroupName: String, enabled: Boolean?): SettingsGroupDto? {
        return azureSettingsDatabaseAccessor.getSettingsGroup(settingsGroupName, enabled)?.let {
            toSettingGroupDto(it)
        }
    }

    override fun getSetting(settingsGroupName: String, settingName: String, enabled: Boolean?): SettingDto? {
        return azureSettingsDatabaseAccessor.getSetting(settingsGroupName, settingName, enabled)?.let {
            toSettingDto(it)
        }
    }

    override fun createOrUpdateSetting(settingGroupName: String, settingDto: SettingDto) {
        azureSettingsDatabaseAccessor.createOrUpdateSetting(settingGroupName, toSetting(settingDto))
    }

    override fun deleteSettingsGroup(settingGroupName: String) {
        azureSettingsDatabaseAccessor.deleteSettingsGroup(settingGroupName)
    }

    override fun deleteSetting(settingGroupName: String, settingName: String) {
        azureSettingsDatabaseAccessor.deleteSetting(settingGroupName, settingName)
        deleteSettingFromCache(settingGroupName, settingName)
    }

    private fun toSettingGroupDto(settingGroup: SettingsGroup): SettingsGroupDto {
        val settingsDtos = settingGroup.settings.map { toSettingDto(it) }.toSet()
        return SettingsGroupDto(settingGroup.name, settingsDtos)
    }

    private fun toSettingDto(setting: Setting): SettingDto {
        return SettingDto(setting.name, setting.value, setting.enabled, setting.comment)
    }

    private fun toSetting(settingDto: SettingDto): Setting {
        return Setting(settingDto.name, settingDto.value, settingDto.enabled, settingDto.comment)
    }

    private fun deleteSettingFromCache(settingGroupName: String, settingName: String) {
        DELETE_SETTING_FROM_CACHE_STRATEGIES[settingGroupName]!!.invoke(settingName)
    }

    private fun deleteSettingGroupFromCache(settingGroupName: String) {

    }
}