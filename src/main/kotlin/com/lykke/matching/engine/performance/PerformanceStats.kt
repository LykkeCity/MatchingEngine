package com.lykke.matching.engine.performance

class PerformanceStats(
        val type: Byte,
        var totalTime: Long,
        var processingTime: Long,
        var count: Long
)