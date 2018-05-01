package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.TypePerformanceStats
import com.lykke.matching.engine.daos.azure.monitoring.AzureMonitoringResult
import com.lykke.matching.engine.daos.azure.monitoring.AzurePerformanceStats
import com.lykke.matching.engine.daos.monitoring.MonitoringResult
import com.lykke.matching.engine.database.MonitoringDatabaseAccessor
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone

class AzureMonitoringDatabaseAccessor(connString: String, monitoringTable: String, performanceTable: String): MonitoringDatabaseAccessor{
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureMonitoringDatabaseAccessor::class.java.name)
    }

    private val monitoringTable: CloudTable = getOrCreateTable(connString, monitoringTable)
    private val performanceStatsTable: CloudTable = getOrCreateTable(connString, performanceTable)

    private val DATE_FORMAT_PARTITION_KEY = initTimeFormatter("yyyyMMdd")
    private val DATE_FORMAT_ROW_KEY = initTimeFormatter("yyyy-MM-dd HH:mm:ss")

    override fun saveMonitoringResult(monitoringResult: MonitoringResult) {
        try {
            val now = Date()
            monitoringTable.execute(TableOperation.insertOrMerge(
                    AzureMonitoringResult(
                            DATE_FORMAT_PARTITION_KEY.format(now),
                            DATE_FORMAT_ROW_KEY.format(now),
                            monitoringResult)))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert monitoring result: ${e.message}")
        }
    }

    override fun savePerformanceStats(stats: TypePerformanceStats) {
        try {
            val now = Date()
            performanceStatsTable.execute(TableOperation.insertOrMerge(
                    AzurePerformanceStats(
                            DATE_FORMAT_PARTITION_KEY.format(now),
                            "${stats.type}_${DATE_FORMAT_ROW_KEY.format(now)}",
                            stats)))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert performance result $stats: ${e.message}")
        }
    }

    private fun initTimeFormatter(formatString: String): SimpleDateFormat {
        val format = SimpleDateFormat(formatString)
        format.timeZone = TimeZone.getTimeZone("UTC")
        return format
    }
}