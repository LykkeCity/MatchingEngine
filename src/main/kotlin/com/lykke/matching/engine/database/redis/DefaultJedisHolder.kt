package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.utils.config.RedisConfig
import com.lykke.utils.logging.ThrottlingLogger
import redis.clients.jedis.JedisPool
import kotlin.concurrent.thread
import redis.clients.jedis.JedisPoolConfig
import java.time.Duration


class DefaultJedisHolder(private val redisConfig: RedisConfig): JedisHolder {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(DefaultJedisHolder::class.java.name)
    }

    private var jedisPool = openRedisConnection()
    private var externalFail = false
    var ok = false

    override fun ok() = ok

    fun fail() {
        externalFail = true
    }

    fun resource() = jedisPool.resource

    fun balanceDatabase() = redisConfig.balanceDatabase

    private fun openRedisConnection(): JedisPool {
        return JedisPool(buildPoolConfig(), redisConfig.host, redisConfig.port, redisConfig.timeout, redisConfig.password, redisConfig.useSsl)
    }

    private fun buildPoolConfig(): JedisPoolConfig {
        val poolConfig = JedisPoolConfig()
        poolConfig.maxTotal = 128
        poolConfig.maxIdle = 128
        poolConfig.minIdle = 16
        poolConfig.testOnBorrow = true
        poolConfig.testOnReturn = true
        poolConfig.testWhileIdle = true
        poolConfig.minEvictableIdleTimeMillis = Duration.ofSeconds(60).toMillis()
        poolConfig.timeBetweenEvictionRunsMillis = Duration.ofSeconds(30).toMillis()
        poolConfig.numTestsPerEvictionRun = 3
        poolConfig.blockWhenExhausted = true
        return poolConfig
    }

    private fun reinit(): Boolean {
        return try {
            try {
                jedisPool.close()
            } catch (e: Exception) {
                // ignored
            }
            jedisPool = openRedisConnection()
            true
        } catch (e: Exception) {
            LOGGER.error("Unable to open redis connection", e)
            false
        }
    }

    private fun ping(): Boolean {
        try {
            jedisPool.resource.use { jedis ->
                return "PONG" == jedis.ping()
            }
        } catch (e: Exception) {
            return false
        }
    }

    init {
        thread(name = DefaultJedisHolder::class.java.name) {
            while (true) {
                var ping = ping()
                if (externalFail || !ping) {
                    ok = false
                    while (!reinit()) {
                        Thread.sleep(100)
                    }
                    externalFail = false
                    ping = ping()
                }
                ok = ping
                Thread.sleep(500)
            }
        }
    }

}