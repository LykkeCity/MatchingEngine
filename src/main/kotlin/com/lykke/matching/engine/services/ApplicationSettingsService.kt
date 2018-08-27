package com.lykke.matching.engine.services

import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto

interface ApplicationSettingsService {
    fun getAllSettingGroups(enabled: Boolean? = null): Set<SettingsGroupDto>

    fun getSettingsGroup(settingsGroupName: String, enabled: Boolean? = null): SettingsGroupDto?
    fun getSetting(settingsGroupName: String, settingName: String, enabled: Boolean? = null): SettingDto?

    fun createOrUpdateSetting(settingGroupName: String, settingDto: SettingDto)

    fun deleteSettingsGroup(settingGroupName: String)
    fun deleteSetting(settingGroupName: String, settingName: String)
}