package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup

interface SettingsDatabaseAccessor {
    fun getSetting(settingGroup: AvailableSettingGroup, settingName: String, enabled: Boolean? = null): Setting?
    fun getSettingsGroup(settingGroup: AvailableSettingGroup, enabled: Boolean? = null): SettingsGroup?
    fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup>

    fun createOrUpdateSetting(settingGroup: AvailableSettingGroup, setting: Setting)

    fun deleteSetting(settingGroup: AvailableSettingGroup, settingName: String)
    fun deleteSettingsGroup(settingGroup: AvailableSettingGroup)
}