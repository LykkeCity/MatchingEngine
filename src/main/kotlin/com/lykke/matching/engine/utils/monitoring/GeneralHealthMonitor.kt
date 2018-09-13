package com.lykke.matching.engine.utils.monitoring

import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.util.CollectionUtils
import java.util.concurrent.ConcurrentHashMap

@Component
class GeneralHealthMonitor: HealthMonitor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(GeneralHealthMonitor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val brokenComponents = ConcurrentHashMap.newKeySet<MonitoredComponent>()

    override fun ok() = brokenComponents.isEmpty()

    @EventListener
    fun processHealthMonitorEvent(event: HealthMonitorEvent) {
        if (event.ok) {
            brokenComponents.remove(event.component)
        } else {
            brokenComponents.add(event.component)
        }
    }

    @Scheduled(fixedRateString = "\${health.check.update.interval}")
    open fun checkBrokenComponents() {
        if (!CollectionUtils.isEmpty(brokenComponents)) {
            val message = "Maintenance mode is on"
            LOGGER.error(message)
            METRICS_LOGGER.logError(message)
        }
    }
}