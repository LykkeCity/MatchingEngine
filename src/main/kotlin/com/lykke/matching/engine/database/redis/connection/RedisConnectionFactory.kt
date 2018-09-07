package com.lykke.matching.engine.database.redis.connection

interface RedisConnectionFactory {
    fun getConnection(name: String): RedisConnection?
}