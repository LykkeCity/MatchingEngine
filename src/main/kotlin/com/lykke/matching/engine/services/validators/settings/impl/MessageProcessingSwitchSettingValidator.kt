package com.lykke.matching.engine.services.validators.settings.impl

import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.settings.SettingValidator
import com.lykke.matching.engine.web.dto.SettingDto
import org.springframework.stereotype.Component

@Component
class MessageProcessingSwitchSettingValidator: SettingValidator {
    companion object {
        val SUPPORTED_VALUE = "stop"
    }

    override fun validate(settingDto: SettingDto) {
        if (settingDto.name != SUPPORTED_VALUE || settingDto.value != SUPPORTED_VALUE) {
            throw ValidationException(validationMessage = "Not acceptable setting value/setting name was supplied. Acceptable setting value/name is: '$SUPPORTED_VALUE'")
        }
    }

    override fun getSettingGroup(): AvailableSettingGroup {
        return AvailableSettingGroup.MESSAGE_PROCESSING_SWITCH
    }
}