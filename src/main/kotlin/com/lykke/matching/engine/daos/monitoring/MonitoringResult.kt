package com.lykke.matching.engine.daos.monitoring

data class MonitoringResult(
        val vmCpuLoad: Double,
        val totalCpuLoad: Double,
        val totalMemory: Long,
        val freeMemory: Long,
        val maxHeap: Long,
        val totalHeap: Long,
        val freeHeap: Long,
        val totalSwap: Long,
        val freeSwap: Long,
        val threadsCount: Int
)