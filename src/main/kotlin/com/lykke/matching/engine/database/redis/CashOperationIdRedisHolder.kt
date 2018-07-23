package com.lykke.matching.engine.database.redis

import redis.clients.jedis.Jedis

interface CashOperationIdRedisHolder: RedisHealthMonitor {
    fun cashOperationIdRedis(): Jedis
}