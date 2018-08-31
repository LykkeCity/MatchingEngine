package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup

/**
 * Enabled flag is not supported for simplicity
 */
class TestSettingsDatabaseAccessor : SettingsDatabaseAccessor {

    private val settingGroups = HashMap<String, SettingsGroup>()

    override fun getSetting(settingGroupName: String, settingName: String, enabled: Boolean?): Setting? {
        return settingGroups[settingGroupName]?.settings?.find { it.name == settingName }
    }

    override fun getSettingsGroup(settingGroupName: String, enabled: Boolean?): SettingsGroup? {
        return settingGroups[settingGroupName]
    }

    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup> {
        return settingGroups.values.toSet()
    }

    override fun createOrUpdateSetting(settingGroupName: String, setting: Setting) {
        val settings = settingGroups[settingGroupName]?.settings?.toMutableSet() ?: HashSet()
        settings.removeIf { it -> it.name == setting.name }
        settings.add(setting)
        settingGroups[settingGroupName] = SettingsGroup(settingGroupName, settings)
    }

    override fun deleteSetting(settingGroupName: String, settingName: String) {
        val settingGroup = settingGroups[settingGroupName] ?: return

        val settings = settingGroup.settings.toMutableSet()
        settings.removeIf { it.name == settingName }
        settingGroups.put(settingGroupName, SettingsGroup(settingGroupName, settings))
    }

    override fun deleteSettingsGroup(settingGroupName: String) {
        settingGroups.remove(settingGroupName)
    }

    fun clear() {
        settingGroups.clear()
    }
}