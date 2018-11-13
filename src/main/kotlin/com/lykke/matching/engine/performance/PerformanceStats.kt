package com.lykke.matching.engine.performance

class PerformanceStats(
        val type: Byte,
        var totalTime: Long,
        var processingTime: Long,
        var count: Long
) {
    var persistTime: Long = 0
    var persistTimeCount: Long = 0
}