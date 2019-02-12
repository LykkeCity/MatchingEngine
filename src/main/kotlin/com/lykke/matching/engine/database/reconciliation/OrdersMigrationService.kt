package com.lykke.matching.engine.database.reconciliation

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.database.utils.mapOrdersToOrderBookPersistenceDataList
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.utils.config.Config
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.util.*

@Component
@Order(2)
class OrdersMigrationService(private val config: Config,
                             initialLoadingRedisConnection: Optional<RedisConnection>,
                             private val persistenceManager: PersistenceManager,
                             private val genericLimitOrderService: GenericLimitOrderService,
                             private val genericStopLimitOrderService: GenericStopLimitOrderService): ApplicationRunner {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(OrdersMigrationService::class.java.name)
    }

    private val fileOrderBookDatabaseAccessor = FileOrderBookDatabaseAccessor(config.me.orderBookPath)
    private val fileStopOrderBookDatabaseAccessor = FileStopOrderBookDatabaseAccessor(config.me.stopOrderBookPath)
    private val redisOrderBookDatabaseAccessor = if (initialLoadingRedisConnection.isPresent)
        RedisOrderBookDatabaseAccessor(initialLoadingRedisConnection.get(), config.me.redis.ordersDatabase)
    else null
    private val redisStopOrderBookDatabaseAccessor = if (initialLoadingRedisConnection.isPresent)
        RedisStopOrderBookDatabaseAccessor(initialLoadingRedisConnection.get(), config.me.redis.ordersDatabase)
    else null

    override fun run(args: ApplicationArguments?) {
        if (!config.me.ordersMigration) {
            return
        }
        if (config.me.storage != Storage.Redis) {
            teeLog("Do not perform migration to files")
            return
        }
        fromFilesToRedis()
    }

    private fun fromFilesToRedis() {
        if (redisOrderBookDatabaseAccessor!!.loadLimitOrders().isNotEmpty()) {
            throw Exception("Orders already exist in redis ${config.me.redis.host}.${config.me.redis.port}/${config.me.redis.ordersDatabase}")
        }
        if (redisStopOrderBookDatabaseAccessor!!.loadStopLimitOrders().isNotEmpty()) {
            throw Exception("Stop orders already exist in redis ${config.me.redis.host}.${config.me.redis.port}/${config.me.redis.ordersDatabase}")
        }
        val startTime = Date().time
        teeLog("Starting orders migration from files to redis; files dirs: ${config.me.orderBookPath}, ${config.me.stopOrderBookPath}" +
                ", redis: ${config.me.redis.host}.${config.me.redis.port}/${config.me.redis.ordersDatabase}")
        val orders = fileOrderBookDatabaseAccessor.loadLimitOrders()
        val stopOrders = fileStopOrderBookDatabaseAccessor.loadStopLimitOrders()
        val loadTime = Date().time
        teeLog("Loaded ${orders.size} orders from files (ms: ${loadTime - startTime})")
        persistenceManager.persist(PersistenceData(null,
                null,
                OrderBooksPersistenceData(mapOrdersToOrderBookPersistenceDataList(orders, LOGGER),
                        orders,
                        emptyList()),
                OrderBooksPersistenceData(mapOrdersToOrderBookPersistenceDataList(stopOrders, LOGGER),
                        stopOrders,
                        emptyList()),
                null))
        genericLimitOrderService.update()
        genericStopLimitOrderService.update()
        val saveTime = Date().time
        teeLog("Saved ${orders.size} orders and ${stopOrders.size} stop orders to redis (ms: ${saveTime - loadTime})")
    }

    private fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }
}