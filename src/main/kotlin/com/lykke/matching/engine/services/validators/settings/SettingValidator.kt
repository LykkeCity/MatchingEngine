package com.lykke.matching.engine.services.validators.settings

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.web.dto.SettingDto

interface SettingValidator {
    fun getSettingGroup(): AvailableSettingGroup
    fun validate(setting: SettingDto)
}