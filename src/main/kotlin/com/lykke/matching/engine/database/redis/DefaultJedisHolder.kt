package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.utils.config.RedisConfig
import com.lykke.utils.logging.ThrottlingLogger
import redis.clients.jedis.Jedis
import kotlin.concurrent.thread

class DefaultJedisHolder(private val redisConfig: RedisConfig): JedisHolder {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(DefaultJedisHolder::class.java.name)
    }

    var jedis = openRedisConnection()
        private set

    private var ok = false

    override fun ok() = ok

    fun balanceDatabase() = redisConfig.balanceDatabase

    private fun openRedisConnection(): Jedis {
        val jedis = Jedis(redisConfig.host, redisConfig.port, redisConfig.timeout, redisConfig.useSsl)
        jedis.connect()
        if (redisConfig.password != null) {
            jedis.auth(redisConfig.password)
        }
        return jedis
    }

    private fun reinit(): Boolean {
        return try {
            jedis = openRedisConnection()
            true
        } catch (e: Exception) {
            LOGGER.error("Unable to open redis connection", e)
            false
        }
    }

    private fun ping(): Boolean {
        return try {
            "PONG" == jedis.ping()
        } catch (e: Exception) {
            false
        }
    }

    init {
        thread(name = DefaultJedisHolder::class.java.name) {
            while (true) {
                var ping = ping()
                if (!ping) {
                    ok = false
                    while (!reinit()) {}
                    ping = ping()
                }
                ok = ping
                Thread.sleep(500)
            }
        }
    }

}