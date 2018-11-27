package com.lykke.matching.engine.services

import com.google.gson.Gson
import com.lykke.matching.engine.daos.DisableFunctionalityRule
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.services.validators.settings.impl.DisabledFunctionalitySettingValidator
import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import com.lykke.matching.engine.web.dto.SettingDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*

@Component
class DisabledFunctionalityRulesServiceImpl : DisabledFunctionalityRulesService {

    @Autowired
    private lateinit var disabledFunctionalitySettingValidator: DisabledFunctionalitySettingValidator

    @Autowired
    private lateinit var applicationSettingsService: ApplicationSettingsService

    @Autowired
    private lateinit var gson: Gson

    override fun create(rule: DisabledFunctionalityRuleDto) {
        disabledFunctionalitySettingValidator.validate(rule)

        applicationSettingsService.createOrUpdateSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES,
                SettingDto(name = UUID.randomUUID().toString(),
                        value = gson.toJson(toDisabledFunctionalityRule(rule)),
                        enabled = rule.enabled,
                        comment = rule.comment,
                        user = rule.user))
    }

    override fun update(id: String, rule: DisabledFunctionalityRuleDto) {
        applicationSettingsService.createOrUpdateSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES,
                SettingDto(name = id,
                        value = gson.toJson(toDisabledFunctionalityRule(rule)),
                        enabled = rule.enabled,
                        comment = rule.comment,
                        user = rule.user))
    }

    override fun getAll(enabled: Boolean?): List<DisabledFunctionalityRuleDto> {
        val result = ArrayList<DisabledFunctionalityRuleDto>()
        val settingsGroup = applicationSettingsService.getSettingsGroup(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, enabled)

        settingsGroup?.let {
            it.settings.forEach {
                result.add(toDisabledFunctionalityRuleDto(gson.toJson(it.value.toByteArray()) as DisableFunctionalityRule, it.name, it.timestamp, enabled))
            }
        }

        return result
    }

    override fun get(id: String, enabled: Boolean?): DisabledFunctionalityRuleDto? {
        return applicationSettingsService.getSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, id, enabled)?.let {
            toDisabledFunctionalityRuleDto(gson.toJson(it.value.toByteArray()) as DisableFunctionalityRule, it.name, it.timestamp, it.enabled)
        }
    }

    override fun history(id: String): List<DisabledFunctionalityRuleDto> {
        val historyRecords = applicationSettingsService.getHistoryRecords(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES.settingGroupName, id)

        return historyRecords
                .map {
                    toDisabledFunctionalityRuleDto(gson.toJson(it.value.toByteArray()) as DisableFunctionalityRule,
                            it.name,
                            it.timestamp,
                            it.enabled,
                            it.user,
                            it.comment)
                }
    }

    override fun delete(id: String, deleteRequest: DeleteSettingRequestDto) {
        applicationSettingsService.deleteSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, id, deleteRequest)
    }

    fun toDisabledFunctionalityRule(disabledFunctionalityRuleDto: DisabledFunctionalityRuleDto): DisableFunctionalityRule {
        return disabledFunctionalityRuleDto.let { rule ->
            DisableFunctionalityRule(rule.assetId?.let { it },
                    rule.assetPairId?.let { it },
                    rule.messageTypeId?.let { MessageType.valueOf(it.toByte()) })
        }
    }

    fun toDisabledFunctionalityRuleDto(rule: DisableFunctionalityRule,
                                       id: String?,
                                       timestamp: Date?,
                                       enabled: Boolean?,
                                       comment: String? = null,
                                       user: String? = null): DisabledFunctionalityRuleDto {
        return DisabledFunctionalityRuleDto(
                id = id,
                assetId = rule.assetId,
                assetPairId = rule.assetPairId,
                messageTypeId = rule.messageType!!.type.toInt(),
                enabled = enabled,
                timestamp = timestamp,
                comment = comment,
                user = user)
    }
}