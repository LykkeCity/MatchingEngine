package com.lykke.matching.engine.performance

import org.springframework.stereotype.Component
import java.util.HashMap

@Component
class PerformanceStatsHolder {

    private var statsMap = HashMap<Byte, PerformanceStats>()

    fun addMessage(type: Byte,
                   inputQueueTime: Long?,
                   preProcessedQueueTime: Long,
                   preProcessingTime: Long?,
                   processingTime: Long,
                   totalTime: Long) {
        val stats = statsMap.getOrPut(type) { PerformanceStats(type, 0, 0, 0) }
        inputQueueTime?.let { stats.inputQueueTime += inputQueueTime }
        preProcessingTime?.let { stats.preProcessingTime += preProcessingTime }

        stats.preProcessedMessageQueueTime += preProcessedQueueTime
        stats.processingTime += processingTime
        stats.totalTime += totalTime
        stats.count++
    }

    fun addPersistTime(type: Byte, persistTime: Long) {
        val stats = statsMap.getOrPut(type) { PerformanceStats(type, 0, 0, 0) }
        stats.persistTime += persistTime
        stats.persistTimeCount++
    }

    fun getStatsAndReset(): Map<Byte, PerformanceStats> {
        val result = statsMap
        statsMap = HashMap()
        return result
    }
}