package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.Setting

interface SettingsHistoryDatabaseAccessor {
    fun add(settingGroupName: String, setting: Setting)
    fun get(settingGroupName: String, settingName: String): List<Setting>
}