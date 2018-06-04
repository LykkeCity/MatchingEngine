package com.lykke.matching.engine.utils.monitoring

interface HealthMonitor {
    fun ok(): Boolean
}