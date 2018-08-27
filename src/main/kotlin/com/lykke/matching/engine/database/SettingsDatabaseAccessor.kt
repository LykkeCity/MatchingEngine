package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup

interface SettingsDatabaseAccessor {
    fun getSetting(settingGroupName: String, settingName: String, enabled: Boolean? = null): Setting?
    fun getSettingsGroup(settingGroupName: String, enabled: Boolean? = null): SettingsGroup?
    fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup>

    fun createOrUpdateSetting(settingGroupName: String, setting: Setting)

    fun deleteSetting(settingGroupName: String, settingName: String)
    fun deleteSettingsGroup(settingGroupName: String)
}