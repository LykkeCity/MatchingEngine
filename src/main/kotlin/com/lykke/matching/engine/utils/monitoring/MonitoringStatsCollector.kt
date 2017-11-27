package com.lykke.matching.engine.utils.monitoring

import com.lykke.matching.engine.daos.monitoring.MonitoringResult
import com.sun.management.OperatingSystemMXBean
import java.lang.management.ManagementFactory

class MonitoringStatsCollector {
    private val MB = 1024 * 1024

    fun collectMonitoringResult(): MonitoringResult {
        val osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean::class.java)
        val vmCpuLoad = osBean.processCpuLoad
        val totalCpuLoad = osBean.systemCpuLoad
        val totalMemory = osBean.totalPhysicalMemorySize / MB
        val freeMemory = osBean.freePhysicalMemorySize / MB
        val totalSwap = osBean.totalSwapSpaceSize / MB
        val freeSwap = osBean.freeSwapSpaceSize / MB

        val threadsCount = Thread.getAllStackTraces().keys.size

        return MonitoringResult(
                vmCpuLoad = vmCpuLoad,
                totalCpuLoad = totalCpuLoad,
                totalMemory = totalMemory,
                freeMemory = freeMemory,
                totalSwap = totalSwap,
                freeSwap = freeSwap,
                threadsCount = threadsCount
            )
    }
}