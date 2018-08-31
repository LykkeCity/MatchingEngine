package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.setting.AvailableSettingGroups
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup
import com.lykke.matching.engine.database.SettingsDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto
import org.springframework.stereotype.Service

@Service
class ApplicationSettingsServiceImpl(private val settingsDatabaseAccessor: SettingsDatabaseAccessor,
                                     private val applicationSettingsCache: ApplicationSettingsCache) : ApplicationSettingsService {
    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroupDto> {
        return settingsDatabaseAccessor.getAllSettingGroups(enabled).map { toSettingGroupDto(it) }.toSet()
    }

    override fun getSettingsGroup(settingsGroup: AvailableSettingGroups, enabled: Boolean?): SettingsGroupDto? {
        return settingsDatabaseAccessor.getSettingsGroup(settingsGroup.settingGroupName, enabled)?.let {
            toSettingGroupDto(it)
        }
    }

    override fun getSetting(settingsGroup: AvailableSettingGroups, settingName: String, enabled: Boolean?): SettingDto? {
        return settingsDatabaseAccessor.getSetting(settingsGroup.settingGroupName, settingName, enabled)?.let {
            toSettingDto(it)
        }
    }

    override fun createOrUpdateSetting(settingsGroup: AvailableSettingGroups, settingDto: SettingDto) {
        settingsDatabaseAccessor.createOrUpdateSetting(settingsGroup.settingGroupName, toSetting(settingDto))
        applicationSettingsCache.createOrUpdateSettingValue(settingsGroup, settingDto.name, settingDto.value)
    }

    override fun deleteSettingsGroup(settingsGroup: AvailableSettingGroups) {
        settingsDatabaseAccessor.deleteSettingsGroup(settingsGroup.settingGroupName)
        applicationSettingsCache.deleteSettingGroup(settingsGroup)
    }

    override fun deleteSetting(settingsGroup: AvailableSettingGroups, settingName: String) {
        settingsDatabaseAccessor.deleteSetting(settingsGroup.settingGroupName, settingName)
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
        return Setting(settingDto.name, settingDto.value, settingDto.enabled!!, settingDto.comment)
    }
}