package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting
import com.lykke.matching.engine.daos.setting.SettingsGroup

/**
 * Enabled flag is not supported for simplicity
 */
class TestSettingsDatabaseAccessor : SettingsDatabaseAccessor {

    private val settingGroups = HashMap<AvailableSettingGroup, SettingsGroup>()

    override fun getSetting(settingGroup: AvailableSettingGroup, settingName: String, enabled: Boolean?): Setting? {
        return settingGroups[settingGroup]?.settings?.find { it.name == settingName }
    }

    override fun getSettingsGroup(settingGroup: AvailableSettingGroup, enabled: Boolean?): SettingsGroup? {
        return settingGroups[settingGroup]
    }

    override fun getAllSettingGroups(enabled: Boolean?): Set<SettingsGroup> {
        return settingGroups.values.toSet()
    }

    override fun createOrUpdateSetting(settingGroup: AvailableSettingGroup, setting: Setting) {
        val settings = settingGroups[settingGroup]?.settings?.toMutableSet() ?: HashSet()
        settings.removeIf { it -> it.name == setting.name }
        settings.add(setting)
        settingGroups[settingGroup] = SettingsGroup(settingGroup, settings)
    }

    override fun deleteSetting(availableSettingGroup: AvailableSettingGroup, settingName: String) {
        val settingGroup = settingGroups[availableSettingGroup] ?: return

        val settings = settingGroup.settings.toMutableSet()
        settings.removeIf { it.name == settingName }
        settingGroups.put(availableSettingGroup, SettingsGroup(availableSettingGroup, settings))
    }

    override fun deleteSettingsGroup(availableSettingGroup: AvailableSettingGroup) {
        settingGroups.remove(availableSettingGroup)
    }

    fun clear() {
        settingGroups.clear()
    }
}