package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.setting.AvailableSettingGroups
import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto

interface ApplicationSettingsService {
    fun getAllSettingGroups(enabled: Boolean? = null): Set<SettingsGroupDto>

    fun getSettingsGroup(settingsGroup: AvailableSettingGroups, enabled: Boolean? = null): SettingsGroupDto?
    fun getSetting(settingsGroup: AvailableSettingGroups, settingName: String, enabled: Boolean? = null): SettingDto?

    fun createOrUpdateSetting(settingsGroup: AvailableSettingGroups, settingDto: SettingDto)

    fun deleteSettingsGroup(settingsGroup: AvailableSettingGroups)
    fun deleteSetting(settingsGroup: AvailableSettingGroups, settingName: String)
}