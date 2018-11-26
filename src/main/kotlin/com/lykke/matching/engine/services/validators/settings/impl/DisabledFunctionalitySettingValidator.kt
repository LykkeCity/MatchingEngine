package com.lykke.matching.engine.services.validators.settings.impl

import com.google.gson.Gson
import com.google.gson.JsonSyntaxException
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.settings.SettingValidator
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import com.lykke.matching.engine.web.dto.SettingDto
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class DisabledFunctionalitySettingValidator(val assetsHolder: AssetsHolder,
                                            val assetsPairsHolder: AssetsPairsHolder): SettingValidator {

    @Autowired
    private lateinit var gson: Gson

    override fun getSettingGroup(): AvailableSettingGroup {
        return AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES
    }

    fun validate(rule: DisabledFunctionalityRuleDto) {
        validateRuleIsNotEmpty(rule)
        validateMessageTypeExist(rule)
        validateAssetExist(rule)
        validateAssetPairIdExist(rule)
    }

    override fun validate(setting: SettingDto) {
        try {
            val rule = gson.fromJson(setting.value, DisabledFunctionalityRuleDto::class.java)
            validate(rule)
        } catch(e: JsonSyntaxException) {
            throw ValidationException(validationMessage = "Invalid json was supplied: ${e.message}")
        }
    }

    private fun validateMessageTypeExist(rule: DisabledFunctionalityRuleDto) {
        rule.messageTypeId?.let {
            if (MessageType.valueOf(rule.messageTypeId.toByte()) == null) {
                throw ValidationException(validationMessage = "Provided message type is not supported")
            }
        }
    }

    private fun validateAssetExist(rule: DisabledFunctionalityRuleDto) {
        rule.assetId?.let {
            if (assetsHolder.getAssetAllowNulls(it) == null) {
                throw ValidationException(validationMessage = "Provided asset does not exist")
            }
        }
    }

    private fun validateAssetPairIdExist(rule: DisabledFunctionalityRuleDto) {
        rule.assetPairId?.let {
            if (assetsPairsHolder.getAssetPairAllowNulls(it) == null) {
                throw ValidationException(validationMessage = "Provided asset pair does not exist")
            }
        }
    }

    private fun validateRuleIsNotEmpty(rule: DisabledFunctionalityRuleDto) {
        if (StringUtils.isEmpty(rule.assetId) &&
                StringUtils.isEmpty(rule.assetPairId) &&
                rule.messageTypeId == null) {
            throw ValidationException(validationMessage = "All values of disabled functionality rule can not be empty")
        }
    }
}