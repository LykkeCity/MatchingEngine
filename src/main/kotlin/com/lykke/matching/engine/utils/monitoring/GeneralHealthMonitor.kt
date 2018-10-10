package com.lykke.matching.engine.utils.monitoring

import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class GeneralHealthMonitor: HealthMonitor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(GeneralHealthMonitor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val monitoredComponentsToQualifiers = HashMap<MonitoredComponent, MutableSet<String>>()

    private var previousMaintenanceModeStatus = false

    @Volatile
    private var ok: Boolean = true

    override fun ok() = ok

    @EventListener
    @Synchronized
    fun processHealthMonitorEvent(event: HealthMonitorEvent) {
        if (event.ok) {
            val qualifiers = monitoredComponentsToQualifiers[event.component] ?: return
            qualifiers.remove(getQualifier(event))
            if (qualifiers.isEmpty()) {
                monitoredComponentsToQualifiers.remove(event.component)
            }
        } else {
            val qualifiers = monitoredComponentsToQualifiers.getOrPut(event.component) { HashSet() }
            qualifiers.add(getQualifier(event))
        }
        ok = monitoredComponentsToQualifiers.isEmpty()
    }

    @Scheduled(fixedRateString = "\${health.check.update.interval}")
    open fun checkBrokenComponents() {
        if (!ok) {
            processMaintenanceModeOn()
        } else if(previousMaintenanceModeStatus) {
            processMaintenanceModeOff()
        }
    }

    private fun processMaintenanceModeOn() {
        previousMaintenanceModeStatus = true
        val message = "Maintenance mode is on, broken component are: ${monitoredComponentsToQualifiers.keys}"
        LOGGER.error(message)
        METRICS_LOGGER.logError(message)
    }

    fun processMaintenanceModeOff() {
        previousMaintenanceModeStatus = false
        val message = "Maintenance mode is off"
        LOGGER.info(message)
        METRICS_LOGGER.logWarning(message)
    }

    private fun getQualifier(event: HealthMonitorEvent): String {
        return "${event.component.name}  ${if(event.qualifier != null) "_" + event.qualifier else  ""}"
    }
}