package com.lykke.matching.engine.services.validators.settings.impl

import com.google.gson.Gson
import com.google.gson.JsonSyntaxException
import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.daos.OperationType
import com.lykke.matching.engine.daos.converters.DisabledFunctionalityRulesConverter.Companion.toDisabledFunctionalityRuleDto
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.settings.SettingValidator
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import com.lykke.matching.engine.web.dto.SettingDto
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class DisabledFunctionalitySettingValidator(val assetsHolder: AssetsHolder,
                                            val assetsPairsHolder: AssetsPairsHolder) : SettingValidator {

    @Autowired
    private lateinit var gson: Gson

    override fun getSettingGroup(): AvailableSettingGroup {
        return AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES
    }

    fun validate(rule: DisabledFunctionalityRuleDto) {
        validateRuleIsNotEmpty(rule)
        validateOperationExists(rule)
        validateAssetExist(rule)
        validateAssetPairIdExist(rule)
    }

    override fun validate(setting: SettingDto) {
        try {
            val rule = gson.fromJson(setting.value, DisabledFunctionalityRule::class.java)
            validate(toDisabledFunctionalityRuleDto(rule))
        } catch (e: JsonSyntaxException) {
            throw ValidationException(validationMessage = "Invalid json was supplied: ${e.message}")
        }
    }

    private fun validateOperationExists(rule: DisabledFunctionalityRuleDto) {
        try {
            rule.operationType?.let {
                OperationType.valueOf(it)
            }
        } catch (e: IllegalArgumentException) {
            throw ValidationException(validationMessage = "Operation does not exist")
        }
    }

    private fun validateAssetExist(rule: DisabledFunctionalityRuleDto) {
        if (StringUtils.isEmpty(rule.assetId)) {
            return
        }

        if (assetsHolder.getAssetAllowNulls(rule.assetId!!) == null) {
            throw ValidationException(validationMessage = "Provided asset does not exist")
        }
    }

    private fun validateAssetPairIdExist(rule: DisabledFunctionalityRuleDto) {
        if (StringUtils.isEmpty(rule.assetPairId)) {
            return
        }

        if (assetsPairsHolder.getAssetPairAllowNulls(rule.assetPairId!!) == null) {
            throw ValidationException(validationMessage = "Provided asset pair does not exist")
        }
    }

    private fun validateRuleIsNotEmpty(rule: DisabledFunctionalityRuleDto) {
        if (StringUtils.isEmpty(rule.assetId) &&
                StringUtils.isEmpty(rule.assetPairId) &&
                rule.operationType == null) {
            throw ValidationException(validationMessage = "All values of disabled functionality rule can not be empty")
        }
    }
}