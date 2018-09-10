package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup

class SettingChangedEvent(val settingGroup: AvailableSettingGroup,
                          val settingName: String,
                          val value: String,
                          val comment: String,
                          val user: String)