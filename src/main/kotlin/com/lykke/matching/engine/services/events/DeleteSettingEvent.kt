package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting

class DeleteSettingEvent(val settingGroup: AvailableSettingGroup,
                         val deletedSetting: Setting,
                         val comment: String,
                         val user: String)