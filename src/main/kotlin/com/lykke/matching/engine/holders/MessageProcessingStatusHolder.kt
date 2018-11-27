package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.DisabledFunctionalityRule
import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import org.springframework.stereotype.Component

@Component
open class MessageProcessingStatusHolder(private val generalHealthMonitor: HealthMonitor,
                                    private val applicationSettingsHolder: ApplicationSettingsHolder,
                                    private val disabledFunctionalityRulesHolder: DisabledFunctionalityRulesHolder) {
    fun isMessageSwitchEnabled(): Boolean {
        return applicationSettingsHolder.isMessageProcessingEnabled()
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