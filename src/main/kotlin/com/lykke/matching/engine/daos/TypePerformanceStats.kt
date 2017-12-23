package com.lykke.matching.engine.daos

import java.util.Date

class TypePerformanceStats(
        val timestamp: Date,
        val type: String,
        val totalTime: String,
        val processingTime: String,
        val count: Long
) {
    override fun toString(): String {
        return "TypePerformanceStats(timestamp=$timestamp, type='$type', totalTime='$totalTime', processingTime='$processingTime', count=$count)"
    }
}