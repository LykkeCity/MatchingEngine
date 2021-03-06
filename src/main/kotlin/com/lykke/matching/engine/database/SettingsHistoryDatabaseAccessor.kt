package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.SettingHistoryRecord

interface SettingsHistoryDatabaseAccessor {
    fun save(settingHistoryRecord: SettingHistoryRecord)
    fun get(settingGroupName: String, settingName: String): List<SettingHistoryRecord>
    fun getAll(settingGroupName: String): List<SettingHistoryRecord>
}