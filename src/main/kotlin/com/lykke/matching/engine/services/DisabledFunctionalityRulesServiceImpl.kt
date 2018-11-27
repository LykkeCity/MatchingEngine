package com.lykke.matching.engine.services

import com.google.gson.Gson
import com.lykke.matching.engine.daos.DisabledFunctionalityRule
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

        settingsGroup?.let { settingsGroup ->
            settingsGroup.settings.forEach { setting ->
                result.add(toDisabledFunctionalityRuleDto(gson.fromJson(setting.value, DisabledFunctionalityRule::class.java), setting.name, setting.timestamp, enabled))
            }
        }

        return result
    }

    override fun get(id: String, enabled: Boolean?): DisabledFunctionalityRuleDto? {
        return applicationSettingsService.getSetting(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES, id, enabled)?.let {
            toDisabledFunctionalityRuleDto(gson.fromJson(it.value, DisabledFunctionalityRule::class.java), it.name, it.timestamp, it.enabled)
        }
    }

    override fun history(id: String): List<DisabledFunctionalityRuleDto> {
        val historyRecords = applicationSettingsService.getHistoryRecords(AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES.settingGroupName, id)

        return historyRecords
                .map {
                    toDisabledFunctionalityRuleDto(gson.fromJson(it.value, DisabledFunctionalityRule::class.java),
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

    fun toDisabledFunctionalityRule(disabledFunctionalityRuleDto: DisabledFunctionalityRuleDto): DisabledFunctionalityRule {
        return disabledFunctionalityRuleDto.let { rule ->
            DisabledFunctionalityRule(rule.assetId,
                    rule.assetPairId,
                    rule.messageTypeId?.let { MessageType.valueOf(it.toByte()) })
        }
    }

    fun toDisabledFunctionalityRuleDto(rule: DisabledFunctionalityRule,
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