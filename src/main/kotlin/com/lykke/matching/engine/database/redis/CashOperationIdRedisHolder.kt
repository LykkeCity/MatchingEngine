package com.lykke.matching.engine.database.redis

import redis.clients.jedis.Jedis

interface CashOperationIdRedisHolder: RedisHolder {
    fun cashOperationIdRedis(): Jedis
}