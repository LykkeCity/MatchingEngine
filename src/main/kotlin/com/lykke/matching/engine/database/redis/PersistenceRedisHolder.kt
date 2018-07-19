package com.lykke.matching.engine.database.redis

import redis.clients.jedis.Jedis

interface PersistenceRedisHolder: RedisHolder {
    fun persistenceRedis(): Jedis
}