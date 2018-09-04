package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.SettingHistoryRecord

interface SettingsHistoryDatabaseAccessor {
    fun save(settingGroupName: String, settingHistoryRecord: SettingHistoryRecord)
    fun get(settingGroupName: String, settingName: String): List<SettingHistoryRecord>
}