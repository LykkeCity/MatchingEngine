package com.lykke.matching.engine.utils.monitoring

import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.*

@Component("GeneralHealthMonitor")
class GeneralHealthMonitor: HealthMonitor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(GeneralHealthMonitor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var ok = false

    private val brokenComponents = EnumSet.noneOf(MonitoredComponent::class.java)

    override fun ok() = ok

    @EventListener
    fun processHealthMonitorEvent(event: HealthMonitorEvent) {
        if (!event.ok) {
            brokenComponents.add(event.componentName)
        } else {
            brokenComponents.remove(event.componentName)
        }
    }

    @Scheduled(fixedRateString = "\${health.check.update.interval}")
    open fun checkBrokenComponents() {
        ok = brokenComponents.isEmpty()
        if (!ok) {
            val message = "Maintenance mode is on"
            LOGGER.error(message)
            METRICS_LOGGER.logError(message)
        }
    }
}