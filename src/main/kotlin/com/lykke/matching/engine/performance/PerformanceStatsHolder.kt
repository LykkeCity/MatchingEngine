package com.lykke.matching.engine.performance

import org.springframework.stereotype.Component
import java.util.HashMap

@Component
class PerformanceStatsHolder {

    private var statsMap = HashMap<Byte, PerformanceStats>()

    fun addMessage(type: Byte,
                   startTimestamp: Long,
                   messagePreProcessorStartTimestamp: Long?,
                   messagePreProcessorEndTimestamp: Long?,
                   startMessageProcessingTime: Long,
                   endMessageProcessingTime: Long) {
        val totalTime = endMessageProcessingTime - startTimestamp
        val processingTime = endMessageProcessingTime - startMessageProcessingTime

        val inputQueueTime = messagePreProcessorStartTimestamp?.let {
            it - startTimestamp
        }

        val preProcessingTime = messagePreProcessorStartTimestamp?.let {
            messagePreProcessorEndTimestamp!! - it
        }

        val preProcessedMessageQueueStartTime = messagePreProcessorEndTimestamp
                ?: startTimestamp
        val preProcessedMessageQueueTime = startMessageProcessingTime - preProcessedMessageQueueStartTime

        addMessage(type, inputQueueTime, preProcessedMessageQueueTime, preProcessingTime, processingTime, totalTime)
    }

    fun addMessage(type: Byte,
                   inputQueueTime: Long?,
                   preProcessedQueueTime: Long,
                   preProcessingTime: Long?,
                   processingTime: Long,
                   writeResponseTime: Long?,
                   totalTime: Long) {
        val stats = statsMap.getOrPut(type) { PerformanceStats(type) }
        inputQueueTime?.let {
            stats.inputQueueTime = stats.inputQueueTime ?: 0
            stats.inputQueueTime = (stats.inputQueueTime as Long) + inputQueueTime
        }

        preProcessingTime?.let {
            stats.preProcessingTime = stats.preProcessingTime ?: 0
            stats.preProcessingTime = (stats.preProcessingTime as Long) + preProcessingTime
        }

        stats.preProcessedMessageQueueTime += preProcessedQueueTime
        stats.processingTime += processingTime

        writeResponseTime?.let {
            stats.writeResponseTime += writeResponseTime
        }
        stats.totalTime += totalTime
        stats.count++
    }

    fun addPersistTime(type: Byte, persistTime: Long) {
        val stats = statsMap.getOrPut(type) { PerformanceStats(type) }
        stats.persistTime += persistTime
        stats.persistsCount++
    }

    fun getStatsAndReset(): Map<Byte, PerformanceStats> {
        val result = statsMap
        statsMap = HashMap()
        return result
    }
}