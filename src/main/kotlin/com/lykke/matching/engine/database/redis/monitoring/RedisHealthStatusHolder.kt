package com.lykke.matching.engine.database.redis.monitoring

import com.lykke.matching.engine.utils.monitoring.HealthMonitor

interface RedisHealthStatusHolder: HealthMonitor {
    fun fail()
}