package com.lykke.matching.engine.database.redis.monitoring.impl

import com.lykke.matching.engine.database.redis.monitoring.RedisHealthStatusHolder
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import redis.clients.jedis.JedisPool
import javax.annotation.PostConstruct
import kotlin.concurrent.fixedRateTimer


@Component
class RedisHealthStatusHolderImpl @Autowired constructor(
        private val jedisPool: JedisPool,
        private val config: Config,
        private val applicationEventPublisher: ApplicationEventPublisher,
        @Value("\${redis.health_check.update.interval}") private val  updateInteval: Long): RedisHealthStatusHolder {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RedisHealthStatusHolderImpl::class.java.name)
        private val PING_KEY = "PING"
        private val PING_VALUE = "PONG"
    }

    private var externalFail = false
    var ok = false

    override fun ok() = ok && !externalFail

    override fun fail() {
        externalFail = true
    }

    private fun getPingDb(): Int {
        return if (config.me.redis.balanceDatabase != 0) 0 else 1
    }

    private fun isRedisAlive(): Boolean {
        try {
            jedisPool.resource.use { jedis ->
                val transaction = jedis.multi()
                try {
                    transaction.select(getPingDb())
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
            LOGGER.error("Redis ping operation failed")
            return false
        }
    }

    @PostConstruct
    open fun init() {
        fixedRateTimer(RedisHealthStatusHolderImpl::class.java.name, false, 0, updateInteval) {
            val redisAlive = isRedisAlive()
            ok = externalFail || redisAlive
            applicationEventPublisher.publishEvent(HealthMonitorEvent(ok, MonitoredComponent.REDIS))
            externalFail = redisAlive
        }
    }
}