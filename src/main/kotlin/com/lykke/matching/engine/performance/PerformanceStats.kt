package com.lykke.matching.engine.performance

class PerformanceStats(
        val type: Byte
) {
    var inputQueueTime: Long? = null
    var preProcessingTime: Long? = null
    var preProcessedMessageQueueTime: Long = 0
    var persistTime: Long = 0
    var persistsCount: Long = 0
    var processingTime: Long = 0
    var writeResponseTime: Long = 0
    var count: Long = 0
    var totalTime: Long = 0
}