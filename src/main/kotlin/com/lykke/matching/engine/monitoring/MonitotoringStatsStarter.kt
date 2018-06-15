package com.lykke.matching.engine.monitoring

import com.lykke.matching.engine.database.MonitoringDatabaseAccessor
import com.lykke.matching.engine.messages.MessageProcessor
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.monitoring.MonitoringStatsCollector
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@Profile("default")
class MonitotoringStatsStarter @Autowired constructor(private val monitoringDatabaseAccessor: MonitoringDatabaseAccessor,
                                                      private val monitoringStatsCollector :MonitoringStatsCollector) {

    @Scheduled(fixedRateString = "\${monitoring.stats.interval}",  initialDelayString = "\${monitoring.stats.interval}")
    private fun start() {
        Thread.currentThread().name = "Monitoring"
        val result = monitoringStatsCollector.collectMonitoringResult()
        if (result != null) {
            MessageProcessor.MONITORING_LOGGER.info("CPU: ${NumberUtils.roundForPrint2(result.vmCpuLoad)}/${NumberUtils.roundForPrint2(result.totalCpuLoad)}, " +
                    "RAM: ${result.freeMemory}/${result.totalMemory}, " +
                    "heap: ${result.freeHeap}/${result.totalHeap}/${result.maxHeap}, " +
                    "swap: ${result.freeSwap}/${result.totalSwap}, " +
                    "threads: ${result.threadsCount}")

            monitoringDatabaseAccessor.saveMonitoringResult(result)
        }
    }
}