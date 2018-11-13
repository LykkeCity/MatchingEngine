package com.lykke.matching.engine.performance

class PerformanceStats(
        val type: Byte,
        var inputQueueTime: Long  = 0,
        var preProcessingTime: Long = 0,
        var preProcessedMessageQueueTime: Long = 0,
        var persistTime: Long = 0,
        var persistTimeCount: Long = 0,
        var processingTime: Long = 0,
        var count: Long = 0,
        var totalTime: Long = 0
)