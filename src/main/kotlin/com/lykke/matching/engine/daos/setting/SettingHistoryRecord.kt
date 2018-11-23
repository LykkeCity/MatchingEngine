package com.lykke.matching.engine.daos.setting

import java.util.*

class SettingHistoryRecord(val settingGroupName: String,
                           val name: String,
                           val value: String,
                           val enabled: Boolean,
                           val comment: String,
                           val user: String,
                           val timestamp: Date? = null)