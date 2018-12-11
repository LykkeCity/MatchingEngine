package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.OperationType
import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import org.springframework.stereotype.Component

@Component
open class MessageProcessingStatusHolder(private val generalHealthMonitor: HealthMonitor,
                                         private val applicationSettingsHolder: ApplicationSettingsHolder,
                                         private val disabledFunctionalityRulesHolder: DisabledFunctionalityRulesHolder) {

    fun isMessageProcessingEnabled(operationType: OperationType): Boolean {
        return isMessageProcessingEnabled(assetPair = null, operationType = operationType)
    }

    fun isMessageProcessingEnabled(assetPair: AssetPair?, operationType: OperationType): Boolean {
        return applicationSettingsHolder.isMessageProcessingEnabled() && !disabledFunctionalityRulesHolder.isDisabled(assetPair, operationType)
    }

    fun isMessageProcessingEnabled(asset: Asset?, operationType: OperationType): Boolean {
        return applicationSettingsHolder.isMessageProcessingEnabled() && !disabledFunctionalityRulesHolder.isDisabled(asset, operationType)
    }

    fun isMessageProcessingEnabled(): Boolean {
        return applicationSettingsHolder.isMessageProcessingEnabled()
    }

    fun isHealthStatusOk(): Boolean {
        return generalHealthMonitor.ok()
    }
}