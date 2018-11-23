package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.SettingDto
import com.lykke.matching.engine.web.dto.SettingsGroupDto

interface ApplicationSettingsService {
    fun getAllSettingGroups(enabled: Boolean? = null): Set<SettingsGroupDto>
    fun getSettingsGroup(settingsGroup: AvailableSettingGroup, enabled: Boolean? = null): SettingsGroupDto?
    fun getSetting(settingsGroup: AvailableSettingGroup, settingName: String, enabled: Boolean? = null): SettingDto?
    fun getHistoryRecords(settingsGroupName: String, settingName: String): List<SettingDto>

    fun createOrUpdateSetting(settingsGroup: AvailableSettingGroup, settingDto: SettingDto)

    fun deleteSettingsGroup(settingsGroup: AvailableSettingGroup, deleteSettingRequestDto: DeleteSettingRequestDto)
    fun deleteSetting(settingsGroup: AvailableSettingGroup, settingName: String, deleteSettingRequestDto: DeleteSettingRequestDto)
}