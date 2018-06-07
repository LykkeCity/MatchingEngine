package com.lykke.matching.engine.database.redis.monitoring.impl

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.redis.monitoring.RedisHealthStatusHolder
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.scheduling.TaskScheduler
import org.springframework.stereotype.Component
import redis.clients.jedis.JedisPool
import javax.annotation.PostConstruct


@Component
class RedisHealthStatusHolderImpl @Autowired constructor(
        private val jedisPool: JedisPool,
        private val config: Config,
        private val applicationEventPublisher: ApplicationEventPublisher,
        private val taskScheduler: TaskScheduler,
        @Value("\${redis.health_check.update.interval}") private val  updateInteval: Long): RedisHealthStatusHolder {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RedisHealthStatusHolderImpl::class.java.name)
        private val PING_KEY = "PING"
        private val PING_VALUE = "PONG"
    }

    private var externalFail = false
    var ok = false

    override fun fail() {
        externalFail = true
    }

    private fun isRedisAlive(): Boolean {
        try {
            jedisPool.resource.use { jedis ->
                val transaction = jedis.multi()
                try {
                    transaction.select(config.me.redis.pingDatabase)
                    transaction[PING_KEY] = PING_VALUE
                    transaction.exec()
                } catch (e: Exception) {
                    LOGGER.error("Redis ping operation failed")
                    transaction.clear()
                    return false
                }
            }
            return true
        } catch (e: Exception) {
            LOGGER.error("Redis ping operation failed", e)
            return false
        }
    }

    @PostConstruct
    fun init() {
        if (config.me.storage != Storage.Redis) {
            return
        }
        taskScheduler.scheduleAtFixedRate ({
            val redisAlive = isRedisAlive()
            ok = externalFail || redisAlive
            applicationEventPublisher.publishEvent(HealthMonitorEvent(ok, MonitoredComponent.REDIS))
            externalFail = redisAlive
        },  updateInteval)
    }
}