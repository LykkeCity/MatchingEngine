package com.lykke.matching.engine.utils.monitoring

import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.*
import javax.annotation.PostConstruct
import kotlin.concurrent.fixedRateTimer

@Component
class GeneralHealthMonitor
@Autowired constructor (@Value("\${health.check.update.interval}") private val updateInterval: Long): HealthMonitor {

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

    @PostConstruct
    open fun init() {
        fixedRateTimer(GeneralHealthMonitor::class.java.name, false, 0, updateInterval) {
            ok = brokenComponents.isEmpty()
            if (!ok) {
                val message = "Maintenance mode is on"
                LOGGER.error(message)
                METRICS_LOGGER.logError(message)
            }
        }
    }
}