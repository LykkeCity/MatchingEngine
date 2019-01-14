package com.lykke.matching.engine.performance

import com.lykke.matching.engine.daos.TypePerformanceStats
import com.lykke.matching.engine.database.MonitoringDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.utils.AppVersion
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.util.Date

@Component
@Profile("default")
class PerformanceStatsLogger @Autowired constructor(private val monitoringDatabaseAccessor: MonitoringDatabaseAccessor) {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(PerformanceStatsLogger::class.java.name)
    }

    fun logStats(stats: Collection<PerformanceStats>) {
        val now = Date()
        stats.forEach { typeStats ->
            val type = MessageType.valueOf(typeStats.type)!!.name
            val inputQueueTime = typeStats.inputQueueTime?.let {
                PrintUtils.convertToString2(it.toDouble() / typeStats.count)
            }
            val preProcessingTime = typeStats.preProcessingTime?.let {
                PrintUtils.convertToString2(it.toDouble() / typeStats.count)
            }
            val preProcessedMessageQueueTime = PrintUtils.convertToString2(typeStats.preProcessedMessageQueueTime.toDouble() / typeStats.count)

            val totalTime = PrintUtils.convertToString2(typeStats.totalTime.toDouble() / typeStats.count)
            val processingTime = PrintUtils.convertToString2(typeStats.processingTime.toDouble() / typeStats.count)
            val persistTime = PrintUtils.convertToString2(typeStats.persistTime.toDouble() / typeStats.persistsCount)
            val writeResponseTime = PrintUtils.convertToString2(typeStats.writeResponseTime.toDouble() / typeStats.count)
            LOGGER.info("App version: ${AppVersion.VERSION}, $type: count: ${typeStats.count}, " +
                    (inputQueueTime?.let { "input queue time: $it, " } ?: "") +
                    (preProcessingTime?.let { "pre processing time: $it, " } ?: "") +
                    "pre processed message queue time: $preProcessedMessageQueueTime, " +
                    "processing time: $processingTime " +
                    "(persist time: $persistTime, write response time: $writeResponseTime), " +
                    "persist count: ${typeStats.persistsCount}, " +
                    "total time: $totalTime")

            monitoringDatabaseAccessor.savePerformanceStats(TypePerformanceStats(timestamp =  now,
                    appVersion =  AppVersion.VERSION,
                    type = type,
                    inputQueueTime =  inputQueueTime,
                    preProcessingTime =  preProcessingTime,
                    preProcessedMessageQueueTime =  preProcessedMessageQueueTime,
                    processingTime =  processingTime,
                    persistTime =  persistTime,
                    writeResponseTime =  writeResponseTime,
                    totalTime =  totalTime,
                    count =  typeStats.count,
                    persistCount =  typeStats.persistsCount))
        }
    }
}