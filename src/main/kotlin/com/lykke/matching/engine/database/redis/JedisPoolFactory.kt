package com.lykke.matching.engine.database.redis

import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.time.Duration

class JedisPoolFactory {

    fun create(host: String, port: Int, dbIndex: Int): JedisPoolHolder {
        return JedisPoolHolder(JedisPool(buildPoolConfig(), host, port), dbIndex)
    }

    private fun buildPoolConfig(): JedisPoolConfig {
        val poolConfig = JedisPoolConfig()
        poolConfig.maxTotal = 1
        poolConfig.maxIdle = 1
        poolConfig.minIdle = 1
        poolConfig.testOnBorrow = true
        poolConfig.testOnReturn = true
        poolConfig.testWhileIdle = true
        poolConfig.minEvictableIdleTimeMillis = Duration.ofSeconds(60).toMillis()
        poolConfig.timeBetweenEvictionRunsMillis = Duration.ofSeconds(30).toMillis()
        poolConfig.numTestsPerEvictionRun = 3
        poolConfig.blockWhenExhausted = true
        return poolConfig
    }
}