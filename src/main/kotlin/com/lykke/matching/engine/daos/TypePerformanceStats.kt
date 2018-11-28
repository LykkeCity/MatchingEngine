package com.lykke.matching.engine.daos

import java.util.Date

class TypePerformanceStats(
        val timestamp: Date,
        val type: String,
        val inputQueueTime: String?,
        val preProcessingTime: String?,
        val preProcessedMessageQueueTime: String,
        val processingTime: String,
        val persistTime: String,
        val totalTime: String,
        val count: Long,
        val persistCount: Long
) {
    override fun toString(): String {
        return "TypePerformanceStats(timestamp=$timestamp, " +
                "type='$type', " +
                "inputQueueTime='$inputQueueTime', " +
                "preProcessingTime='$preProcessingTime', " +
                "preProcessedMessageQueueTime='$preProcessedMessageQueueTime', " +
                "processingTime='$processingTime', " +
                "persistTime='$persistTime', " +
                "totalTime='$totalTime', " +
                "count=$count, " +
                "persistCount=$persistCount)"
    }
}