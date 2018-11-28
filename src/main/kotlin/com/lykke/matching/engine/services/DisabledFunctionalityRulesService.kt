package com.lykke.matching.engine.services

import com.lykke.matching.engine.web.dto.DeleteSettingRequestDto
import com.lykke.matching.engine.web.dto.DisabledFunctionalityRuleDto

interface DisabledFunctionalityRulesService {
    fun create(rule: DisabledFunctionalityRuleDto)
    fun update(id: String, rule: DisabledFunctionalityRuleDto)
    fun getAll(enabled: Boolean? = null): List<DisabledFunctionalityRuleDto>
    fun get(id: String): DisabledFunctionalityRuleDto?
    fun history(id: String): List<DisabledFunctionalityRuleDto>
    fun delete(id: String, deleteRequest: DeleteSettingRequestDto)
}