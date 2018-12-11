package com.lykke.matching.engine.services.events

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.setting.Setting

class ApplicationSettingCreatedOrUpdatedEvent(val settingGroup: AvailableSettingGroup,
                                              val setting: Setting,
                                              val previousSetting: Setting?,
                                              val comment: String,
                                              val user: String)