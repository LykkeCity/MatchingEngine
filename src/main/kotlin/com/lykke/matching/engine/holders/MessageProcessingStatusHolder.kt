package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import org.springframework.stereotype.Component

@Component
class MessageProcessingStatusHolder(private val generalHealthMonitor: HealthMonitor,
                                    private val applicationSettingsHolder: ApplicationSettingsHolder) {
    fun isMessageSwitchEnabled(): Boolean {
        return applicationSettingsHolder.isMessageProcessingEnabled()
    }

    fun isHealthStatusOk(): Boolean {
        return generalHealthMonitor.ok()
    }
}