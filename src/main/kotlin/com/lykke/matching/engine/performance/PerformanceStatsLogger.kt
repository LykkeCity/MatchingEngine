package com.lykke.matching.engine.performance

import com.lykke.matching.engine.daos.TypePerformanceStats
import com.lykke.matching.engine.database.MonitoringDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.util.Date

@Component
@Profile("default")
class PerformanceStatsLogger @Autowired constructor (private val monitoringDatabaseAccessor: MonitoringDatabaseAccessor) {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(PerformanceStatsLogger::class.java.name)
    }

    fun logStats(stats: Collection<PerformanceStats>) {
        val now = Date()
        stats.forEach { typeStats ->
            val type = MessageType.valueOf(typeStats.type)!!.name
            val totalTime = PrintUtils.convertToString2(typeStats.totalTime.toDouble() / typeStats.count)
            val processingTime = PrintUtils.convertToString2(typeStats.processingTime.toDouble() / typeStats.count)
            LOGGER.info("$type: count: ${typeStats.count}, total time: $totalTime, processing time: $processingTime")

            monitoringDatabaseAccessor.savePerformanceStats(TypePerformanceStats(now, type, totalTime, processingTime, typeStats.count))
        }
    }
}