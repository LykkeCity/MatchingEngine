package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.setting.AvailableSettingGroups
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

    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroupDto> {
        return azureSettingsDatabaseAccessor.getAllSettingGroups(enabled).map { toSettingGroupDto(it) }.toSet()
    }

    override fun getSettingsGroup(settingsGroup: AvailableSettingGroups, enabled: Boolean?): SettingsGroupDto? {
        return azureSettingsDatabaseAccessor.getSettingsGroup(settingsGroup.name, enabled)?.let {
            toSettingGroupDto(it)
        }
    }

    override fun getSetting(settingsGroup: AvailableSettingGroups, settingName: String, enabled: Boolean?): SettingDto? {
        return azureSettingsDatabaseAccessor.getSetting(settingsGroup.name, settingName, enabled)?.let {
            toSettingDto(it)
        }
    }

    override fun createOrUpdateSetting(settingsGroup: AvailableSettingGroups, settingDto: SettingDto) {
        azureSettingsDatabaseAccessor.createOrUpdateSetting(settingsGroup.name, toSetting(settingDto))
        applicationSettingsCache.createOrUpdateSettingValue(settingsGroup, settingDto.name, settingDto.value)
    }

    override fun deleteSettingsGroup(settingsGroup: AvailableSettingGroups) {

        azureSettingsDatabaseAccessor.deleteSettingsGroup(settingsGroup.name)
        applicationSettingsCache.deleteSettingGroup(settingsGroup)
    }

    override fun deleteSetting(settingsGroup: AvailableSettingGroups, settingName: String) {
        azureSettingsDatabaseAccessor.deleteSetting(settingsGroup.name, settingName)
        applicationSettingsCache.deleteSetting(settingsGroup, settingName)
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
}