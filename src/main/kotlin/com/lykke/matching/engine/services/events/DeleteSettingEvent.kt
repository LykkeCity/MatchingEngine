package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup

class DeleteSettingEvent(val settingGroup: AvailableSettingGroup,
                         val settingName: String,
                         val comment: String,
                         val user: String)