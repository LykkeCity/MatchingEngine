package com.lykke.matching.engine.database.redis

import redis.clients.jedis.Jedis

interface PersistenceRedisHolder: RedisHealthMonitor {
    fun persistenceRedis(): Jedis
}