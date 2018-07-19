package com.lykke.matching.engine.utils.migration

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.accessor.impl.RedisOrderBookDatabaseAccessor
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.config.Config
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import redis.clients.jedis.JedisPool
import java.util.Date

@Service
class OrdersMigrationService(private val config: Config,
                             jedisPool: JedisPool,
                             private val persistenceManager: PersistenceManager,
                             private val genericLimitOrderService: GenericLimitOrderService) {
    companion object {
        private val LOGGER = Logger.getLogger(OrdersMigrationService::class.java.name)
    }

    private val fileDatabaseAccessor = FileOrderBookDatabaseAccessor(config.me.orderBookPath)
    private val redisDatabaseAccessor = RedisOrderBookDatabaseAccessor(jedisPool, config.me.redis.ordersDatabase)

    fun migrateOrdersIfConfigured() {
        if (!config.me.ordersMigration) {
            return
        }
        if (config.me.storage == Storage.Azure) {
            teeLog("Do not perform migration to files")
            return
        }
        fromFilesToRedis()
    }

    private fun fromFilesToRedis() {
        if (redisDatabaseAccessor.loadLimitOrders().isNotEmpty()) {
            throw Exception("Orders already exist in redis ${config.me.redis.host}.${config.me.redis.port}.${config.me.redis.ordersDatabase}")
        }
        val startTime = Date().time
        teeLog("Starting orders migration from files to redis; files dir: ${config.me.orderBookPath}, redis: ${config.me.redis.host}.${config.me.redis.port}.${config.me.redis.ordersDatabase}")
        val orders = fileDatabaseAccessor.loadLimitOrders()
        val loadTime = Date().time
        teeLog("Loaded ${orders.size} orders from files (ms: ${loadTime - startTime})")
        persistenceManager.persist(PersistenceData(null,
                null,
                OrderBooksPersistenceData(RedisPersistenceManager.mapOrdersToOrderBookPersistenceDataList(orders),
                        orders,
                        emptyList()),
                null,
                null))
        genericLimitOrderService.update()
        val saveTime = Date().time
        teeLog("Saved ${orders.size} orders to redis (ms: ${saveTime - loadTime})")
    }

    private fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }
}