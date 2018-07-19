package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.redis.CashOperationIdRedisHolder
import com.lykke.matching.engine.database.redis.InitialLoadingRedisHolder
import com.lykke.matching.engine.database.redis.PersistenceRedisHolder
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.scheduling.TaskScheduler
import org.springframework.stereotype.Component
import redis.clients.jedis.Jedis
import javax.annotation.PostConstruct

@Component
class DefaultRedisHolder(private val config: Config,
                         private val taskScheduler: TaskScheduler,
                         private val applicationEventPublisher: ApplicationEventPublisher,
                         @Value("\${redis.health.check.interval}")
                         private val updateInterval: Long,
                         @Value("\${redis.health.check.reconnect.interval}")
                         private val reconnectInterval: Long) :
        PersistenceRedisHolder,
        InitialLoadingRedisHolder,
        CashOperationIdRedisHolder {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(DefaultRedisHolder::class.java.name)
        private const val PING_KEY = "PING"
        private const val PING_VALUE = "PONG"
    }

    private lateinit var pingRedis: Jedis
    private lateinit var persistenceRedis: Jedis
    private lateinit var cashOperationIdRedis: Jedis
    private var externalFail = false

    override fun persistenceRedis() = persistenceRedis

    override fun cashOperationIdRedis() = cashOperationIdRedis

    override fun initialLoadingRedis() = persistenceRedis

    override fun fail() {
        applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.REDIS))
        externalFail = true
    }

    private fun initConnections() {
        try {
            pingRedis = openRedisConnection()
            persistenceRedis = openRedisConnection()
            cashOperationIdRedis = openRedisConnection()
        } catch (e: Exception) {
            LOGGER.error("Redis connections initialisation is failed", e)
        }
    }

    private fun openRedisConnection(): Jedis {
        val redisConfig = config.me.redis
        val jedis = Jedis(redisConfig.host, redisConfig.port, redisConfig.timeout, redisConfig.useSsl)
        jedis.connect()
        if (redisConfig.password != null) {
            jedis.auth(redisConfig.password)
        }
        return jedis
    }

    private fun isRedisAlive(): Boolean {
        return try {
            val transaction = pingRedis.multi()
            try {
                transaction.select(config.me.redis.pingDatabase)
                transaction[PING_KEY] = PING_VALUE
                transaction.exec()
                true
            } catch (e: Exception) {
                try {
                    transaction.clear()
                } catch (clearTxException: Exception) {
                    e.addSuppressed(clearTxException)
                }
                throw e
            }
        } catch (e: Exception) {
            LOGGER.error("Redis ping operation failed", e)
            false
        }
    }

    @PostConstruct
    fun init() {
        if (config.me.storage != Storage.Redis) {
            return
        }
        initConnections()
        taskScheduler.scheduleWithFixedDelay(::checkConnection, updateInterval)
    }

    private fun checkConnection() {
        val externalFail = this.externalFail
        var isAlive = !externalFail && isRedisAlive()
        applicationEventPublisher.publishEvent(HealthMonitorEvent(isAlive, MonitoredComponent.REDIS))
        while (!isAlive) {
            initConnections()
            isAlive = isRedisAlive()
            Thread.sleep(reconnectInterval)
        }
        if (externalFail) {
            this.externalFail = false
        }
    }
}