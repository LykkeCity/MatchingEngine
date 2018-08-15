package com.lykke.matching.engine.database.redis

import redis.clients.jedis.Jedis

interface CashTransferOperationIdRedisHolder: RedisHealthMonitor {
    fun cashTransferOperationIdRedis(): Jedis
}