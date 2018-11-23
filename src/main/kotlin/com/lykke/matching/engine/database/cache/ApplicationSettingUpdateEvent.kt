package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting

class ApplicationSettingUpdateEvent (val settingGroup: AvailableSettingGroup, val setting: Setting)