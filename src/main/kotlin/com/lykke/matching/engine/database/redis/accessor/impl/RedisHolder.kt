package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.redis.CashInOutOperationIdRedisHolder
import com.lykke.matching.engine.database.redis.CashTransferOperationIdRedisHolder
import com.lykke.matching.engine.database.redis.InitialLoadingRedisHolder
import com.lykke.matching.engine.database.redis.PersistenceRedisHolder
import com.lykke.matching.engine.utils.config.MatchingEngineConfig
import com.lykke.matching.engine.utils.monitoring.HealthMonitorEvent
import com.lykke.matching.engine.utils.monitoring.MonitoredComponent
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.context.ApplicationEventPublisher
import org.springframework.scheduling.TaskScheduler
import redis.clients.jedis.Jedis
import javax.annotation.PostConstruct

class RedisHolder(private val config: MatchingEngineConfig,
                  private val taskScheduler: TaskScheduler,
                  private val applicationEventPublisher: ApplicationEventPublisher,
                  private val updateInterval: Long,
                  private val reconnectInterval: Long) :
        PersistenceRedisHolder,
        InitialLoadingRedisHolder,
        CashInOutOperationIdRedisHolder,
        CashTransferOperationIdRedisHolder{

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(RedisHolder::class.java.name)
        private const val PING_KEY = "PING"
        private const val PING_VALUE = "PONG"
    }

    private lateinit var pingRedis: Jedis
    @Volatile
    private lateinit var persistenceRedis: Jedis
    @Volatile
    private lateinit var cashInOutOperationIdRedis: Jedis
    @Volatile
    private lateinit var cashTransferOperationIdRedis: Jedis
    @Volatile
    private var externalFail = false

    override fun persistenceRedis() = persistenceRedis

    override fun cashInOutOperationIdRedis() = cashInOutOperationIdRedis

    override fun cashTransferOperationIdRedis() = cashTransferOperationIdRedis

    override fun initialLoadingRedis() = persistenceRedis

    override fun fail() {
        applicationEventPublisher.publishEvent(HealthMonitorEvent(false, MonitoredComponent.REDIS))
        externalFail = true
    }

    private fun initConnections() {
        try {
            pingRedis = openRedisConnection()
            persistenceRedis = openRedisConnection()
            cashInOutOperationIdRedis = openRedisConnection()
            cashTransferOperationIdRedis = openRedisConnection()
        } catch (e: Exception) {
            LOGGER.error("Redis connections initialisation is failed", e)
        }
    }

    private fun openRedisConnection(): Jedis {
        val redisConfig = config.redis
        val jedis = Jedis(redisConfig.host, redisConfig.port, redisConfig.timeout, redisConfig.useSsl)
        jedis.connect()
        if (redisConfig.password != null) {
            jedis.auth(redisConfig.password)
        }
        return jedis
    }

    private fun isRedisAlive(): Boolean {
        return try {
            pingRedis.multi().use { transaction ->
                transaction.select(config.redis.pingDatabase)
                transaction[PING_KEY] = PING_VALUE
                transaction.exec()
            }
            true
        } catch (e: Exception) {
            LOGGER.error("Redis ping operation failed", e)
            false
        }
    }

    @PostConstruct
    fun init() {
        if (config.storage != Storage.Redis) {
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