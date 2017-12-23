package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.TypePerformanceStats
import com.lykke.matching.engine.daos.monitoring.MonitoringResult

interface MonitoringDatabaseAccessor {
    fun saveMonitoringResult(monitoringResult: MonitoringResult)
    fun savePerformanceStats(stats: TypePerformanceStats)
}