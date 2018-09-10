package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup

class DeleteSettingGroupEvent (val settingGroup: AvailableSettingGroup,
                               val comment: String,
                               val user: String)