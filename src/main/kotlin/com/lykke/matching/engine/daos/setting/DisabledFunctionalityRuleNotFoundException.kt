package com.lykke.matching.engine.daos.setting

import com.lykke.matching.engine.exception.MatchingEngineException

class DisabledFunctionalityRuleNotFoundException(ruleId: String): MatchingEngineException("Disabled functionality rule with id: '$ruleId' is not found")
