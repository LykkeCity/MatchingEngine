package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.SettingHistoryRecord

interface SettingsHistoryDatabaseAccessor {
    fun save(settingHistoryRecord: SettingHistoryRecord)
    fun get(settingGroupName: AvailableSettingGroup, settingName: String): List<SettingHistoryRecord>
}