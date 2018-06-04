package com.lykke.matching.engine.utils.monitoring

import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import kotlin.concurrent.thread

class GeneralHealthMonitor(checkers: Collection<HealthMonitor>): HealthMonitor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(GeneralHealthMonitor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private var ok = false

    override fun ok() = ok

    init {
        thread(name = GeneralHealthMonitor::class.java.name) {
            while (true) {
                ok = !checkers.any { !it.ok() }
                if (!ok) {
                    val message = "Maintenance mode is on"
                    LOGGER.error(message)
                    METRICS_LOGGER.logError(message)
                }
                Thread.sleep(100)
            }
        }
    }
}