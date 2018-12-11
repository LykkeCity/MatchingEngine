package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting

class ApplicationSettingDeletedEvent(val settingGroup: AvailableSettingGroup,
                                     val deletedSetting: Setting,
                                     val comment: String,
                                     val user: String)