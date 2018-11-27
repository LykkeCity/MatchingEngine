package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import org.springframework.stereotype.Component

@Component
open class MessageProcessingStatusHolder(private val generalHealthMonitor: HealthMonitor,
                                    private val applicationSettingsCache: ApplicationSettingsCache,
                                    private val disabledFunctionalityRulesHolder: DisabledFunctionalityRulesHolder) {
    fun isMessageProcessingEnabled(disabledFunctionalityRule: DisabledFunctionalityRule? = null): Boolean {
        return applicationSettingsCache.isMessageProcessingEnabled() && !isDisabledFunctionalityRuleMatched(disabledFunctionalityRule)
    }

    fun isHealthStatusOk(): Boolean {
        return generalHealthMonitor.ok()
    }

    fun isDisabledFunctionalityRuleMatched(disabledFunctionalityRule: DisabledFunctionalityRule?): Boolean {
        if (disabledFunctionalityRule == null) {
            return false
        }

        return disabledFunctionalityRulesHolder.isDisabled(disabledFunctionalityRule)
    }
}