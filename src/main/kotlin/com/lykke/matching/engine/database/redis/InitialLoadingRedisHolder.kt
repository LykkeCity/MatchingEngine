package com.lykke.matching.engine.database.redis

import redis.clients.jedis.Jedis

interface InitialLoadingRedisHolder: RedisHolder {
    fun initialLoadingRedis(): Jedis
}