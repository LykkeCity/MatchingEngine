package com.lykke.matching.engine.database.redis

import redis.clients.jedis.Jedis

interface CashInOutOperationIdRedisHolder: RedisHealthMonitor {
    fun cashInOutOperationIdRedis(): Jedis
}