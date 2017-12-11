package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.monitoring.MonitoringResult

interface MonitoringDatabaseAccessor {
    fun saveMonitoringResult(monitoringResult: MonitoringResult)
}