package com.lykke.matching.engine.daos.converters

import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.daos.OperationType
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto
import java.util.*

class DisabledFunctionalityRulesConverter {
    companion object {
        fun toDisabledFunctionalityRule(disabledFunctionalityRuleDto: DisabledFunctionalityRuleDto): DisabledFunctionalityRule {
            return disabledFunctionalityRuleDto.let { rule ->
                DisabledFunctionalityRule(rule.assetId,
                        rule.assetPairId,
                        rule.operationType?.let { OperationType.valueOf(it) })
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
                    operationType = rule.operationType?.let { it.name },
                    enabled = enabled,
                    timestamp = timestamp,
                    comment = comment,
                    user = user)
        }

        fun toDisabledFunctionalityRuleDto(rule: DisabledFunctionalityRule): DisabledFunctionalityRuleDto {
            return toDisabledFunctionalityRuleDto(rule, null, null, null, null, null)
        }
    }
}