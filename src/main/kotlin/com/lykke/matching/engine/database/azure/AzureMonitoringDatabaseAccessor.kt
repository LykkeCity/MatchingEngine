package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.monitoring.AzureMonitoringResult
import com.lykke.matching.engine.daos.monitoring.MonitoringResult
import com.lykke.matching.engine.database.MonitoringDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone

class AzureMonitoringDatabaseAccessor(connString: String): MonitoringDatabaseAccessor{
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureMonitoringDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val monitoringTable: CloudTable = getOrCreateTable(connString, "MatchingEngineMonitoring")

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

    private fun initTimeFormatter(formatString: String): SimpleDateFormat {
        val format = SimpleDateFormat(formatString)
        format.timeZone = TimeZone.getTimeZone("UTC")
        return format
    }
}