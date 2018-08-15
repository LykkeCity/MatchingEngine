package com.lykke.matching.engine.database.redis.connection.impl

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.database.redis.events.RedisFailureEvent
import com.lykke.matching.engine.utils.config.MatchingEngineConfig
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.scheduling.TaskScheduler
import javax.annotation.PostConstruct

class RedisReconnectionManager(private val config: MatchingEngineConfig,
                               private val allRedisConnections: List<RedisConnection>,
                               private val pingRedisConnection: RedisConnection,
                               private val taskScheduler: TaskScheduler,
                               private val applicationEventPublisher: ApplicationEventPublisher,
                               private val updateInterval: Long,
                               private val reconnectInterval: Long) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RedisReconnectionManager::class.java.name)
        private const val PING_KEY = "PING"
        private const val PING_VALUE = "PONG"
    }

    @Volatile
    private var externalFail: Boolean = false

    private fun isRedisAlive(): Boolean {
        return try {
            pingRedisConnection.transactionalResource { transaction ->
                transaction.select(config.redis.pingDatabase)
                transaction[PING_KEY] = PING_VALUE
                transaction.exec()
            }
            true
        } catch (e: Exception) {
            false
        }
    }

    private fun reconnectAll() {
        try {
            allRedisConnections.forEach { it.reconnect() }
        } catch (e: Exception) {
            LOGGER.error("Redis reconnection error,", e)
        }
    }

    @PostConstruct
    fun init() {
        if (config.storage != Storage.Redis) {
            return
        }

        taskScheduler.scheduleWithFixedDelay(::checkConnection, updateInterval)
    }

    @EventListener
    private fun externalRedisOperationFailed(redisFailureEvent: RedisFailureEvent) {
        if (!externalFail) {
            LOGGER.error("Got redis failure event from connection: ${redisFailureEvent.connectionName}")
        }

        externalFail = true
        applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.REDIS))
    }

    private fun performReconnection() {
        var isReconnectionNeeded = true

        while (isReconnectionNeeded) {
            reconnectAll()
            isReconnectionNeeded = !isRedisAlive()
            Thread.sleep(reconnectInterval)
        }

        applicationEventPublisher.publishEvent(HealthMonitorEvent(true, MonitoredComponent.REDIS))
        externalFail = false
    }

    private fun checkConnection() {
        if (externalFail || !isRedisAlive()) {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.REDIS))
            performReconnection()
        }
    }
}