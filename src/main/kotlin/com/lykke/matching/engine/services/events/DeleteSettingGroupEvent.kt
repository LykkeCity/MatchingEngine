package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting

class DeleteSettingGroupEvent (val settingGroup: AvailableSettingGroup,
                               val deletedSettings: Set<Setting>,
                               val comment: String,
                               val user: String)