package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.common.Listener
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.azure.*
import com.lykke.matching.engine.database.common.DefaultPersistenceManager
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.listeners.WalletOperationsPersistListener
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.accessor.impl.RedisCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.monitoring.RedisHealthStatusHolder
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.util.*
import java.util.concurrent.BlockingQueue

@Configuration
open class DatabaseAccessorConfig {

    @Autowired
    private lateinit var config: Config

    @Bean
    open fun persistenceManager(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                                jedisPool: Optional<JedisPool>,
                                redisHealthStatusHolder: RedisHealthStatusHolder,
                                redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
                                cashOperationIdDatabaseAccessor: Optional<CashOperationIdDatabaseAccessor>,
                                messageSequenceNumberDatabaseAccessor: Optional<ReadOnlyMessageSequenceNumberDatabaseAccessor>,
                                updatedWalletsQueue: BlockingQueue<Collection<Wallet>>): PersistenceManager {
        return when (config.me.storage) {
            Storage.Azure -> DefaultPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor, fileProcessedMessagesDatabaseAccessor())
            Storage.Redis -> {
                RedisPersistenceManager(
                        balancesDatabaseAccessorsHolder.primaryAccessor as RedisWalletDatabaseAccessor,
                        redisProcessedMessagesDatabaseAccessor.get(),
                        cashOperationIdDatabaseAccessor.get() as RedisCashOperationIdDatabaseAccessor,
                        redisHealthStatusHolder,
                        messageSequenceNumberDatabaseAccessor.get() as RedisMessageSequenceNumberDatabaseAccessor,
                        jedisPool.get(),
                        updatedWalletsQueue,
                        config
                )
            }
        }
    }

    @Bean
    open fun walletOperationsPersistListener(updatedWalletsQueue: BlockingQueue<Collection<Wallet>>,
                                             balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder): Listener<Collection<Wallet>>? {
        return balancesDatabaseAccessorsHolder.secondaryAccessor?.let {
            WalletOperationsPersistListener(balancesDatabaseAccessorsHolder.secondaryAccessor)
        }
    }

    @Bean
    open fun readOnlyProcessedMessagesDatabaseAccessor(jedisPool: Optional<JedisPool>): ReadOnlyProcessedMessagesDatabaseAccessor {
        return when (config.me.storage) {
            Storage.Azure -> fileProcessedMessagesDatabaseAccessor()
            Storage.Redis -> RedisProcessedMessagesDatabaseAccessor(jedisPool.get(),
                    config.me.redis.processedMessageDatabase,
                    getProcessedMessageTTL())
        }
    }

    @Bean
    open fun jedisPool(@Value("\${redis.max.total}") maxTotal: Int,
                       @Value("\${redis.max.idle}") maxIdle: Int,
                       @Value("\${redis.min.idle}") minIdle: Int,
                       @Value("\${redis.test_on_borrow}") testOnBorrow: Boolean,
                       @Value("\${redis.test_on_return}") testOnReturn: Boolean,
                       @Value("\${redis.test_while_idle}") testWhileIdle: Boolean,
                       @Value("\${redis.min_evictable_idle_time_millis}") minEvictableIdleTimeMillis: Long,
                       @Value("\${redis.time_between_eviction_runs_millis}") timeBetweenEvictionRunsMillis: Long,
                       @Value("\${redis.num_tests_per_eviction_run}") numTestsPerEvictionRun: Int,
                       @Value("\${redis.block_when_exhausted}") blockWhenExhausted: Boolean): JedisPool? {
        val redisConfig = config.me.redis

        if (redisConfig == null) {
            return null
        }

        val poolConfig = JedisPoolConfig()
        poolConfig.maxTotal = maxTotal
        poolConfig.maxIdle = maxIdle
        poolConfig.minIdle = minIdle
        poolConfig.testOnBorrow = testOnBorrow
        poolConfig.testOnReturn = testOnReturn
        poolConfig.testWhileIdle = testWhileIdle
        poolConfig.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis
        poolConfig.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis
        poolConfig.numTestsPerEvictionRun = numTestsPerEvictionRun
        poolConfig.blockWhenExhausted = blockWhenExhausted

        return JedisPool(poolConfig,
                redisConfig.host,
                redisConfig.port,
                redisConfig.timeout,
                redisConfig.password,
                redisConfig.useSsl)
    }

    @Bean
    open fun redisProcessedMessagesDatabaseAccessor(jedisPool: Optional<JedisPool>): RedisProcessedMessagesDatabaseAccessor? {
        if (!jedisPool.isPresent) {
            return null
        }
        return RedisProcessedMessagesDatabaseAccessor(jedisPool.get(),
                config.me.redis.processedMessageDatabase,
                getProcessedMessageTTL())
    }

    @Bean
    open fun cashOperationIdDatabaseAccessor(jedisPool: Optional<JedisPool>): CashOperationIdDatabaseAccessor? {
        return when (config.me.storage) {
            Storage.Azure -> AzureCashOperationIdDatabaseAccessor()
            Storage.Redis -> {
                if (!jedisPool.isPresent) {
                    return null
                }
                return RedisCashOperationIdDatabaseAccessor(jedisPool.get(),
                        config.me.redis.processedCashMessageDatabase)
            }
        }
    }

    @Bean
    open fun messageSequenceNumberDatabaseAccessor(jedisPool: Optional<JedisPool>): ReadOnlyMessageSequenceNumberDatabaseAccessor {
        return when (config.me.storage) {
            Storage.Azure -> AzureMessageSequenceNumberDatabaseAccessor()
            Storage.Redis -> {
                RedisMessageSequenceNumberDatabaseAccessor(jedisPool.get(),
                        config.me.redis.sequenceNumberDatabase)
            }
        }
    }

    @Bean
    open fun backOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
        return AzureBackOfficeDatabaseAccessor(config.me.db.dictsConnString)
    }

    @Bean
    open fun azureCashOperationsDatabaseAccessor( @Value("\${azure.cache.operation.table}") tableName: String)
            : CashOperationsDatabaseAccessor {
        return AzureCashOperationsDatabaseAccessor(config.me.db.balancesInfoConnString, tableName)
    }

    @Bean
    open fun azureHistoryTicksDatabaseAccessor(@Value("\${application.tick.frequency}") frequency: Long)
            : HistoryTicksDatabaseAccessor {
        return AzureHistoryTicksDatabaseAccessor(config.me.db.hLiquidityConnString, frequency)
    }

    @Bean
    open fun azureLimitOrderDatabaseAccessor(@Value("\${azure.best.price.table}") bestPricesTable : String,
                                             @Value("\${azure.candles.table}")candlesTable: String,
                                             @Value("\${azure.hour.candles.table}")hourCandlesTable: String)
            : LimitOrderDatabaseAccessor {
        return AzureLimitOrderDatabaseAccessor(connectionString = config.me.db.hLiquidityConnString,
                bestPricesTable = bestPricesTable, candlesTable = candlesTable, hourCandlesTable = hourCandlesTable)
    }

    @Bean
    open fun azureMarketOrderDatabaseAccessor(@Value("\${azure.market.order.table}") tableName: String)
            : MarketOrderDatabaseAccessor {
        return AzureMarketOrderDatabaseAccessor(config.me.db.hTradesConnString, tableName)
    }


    @Bean
    open fun azureReservedVolumesDatabaseAccessor(@Value("\${azure.reserved.volumes.table}") tableName: String)
            : ReservedVolumesDatabaseAccessor {
        return AzureReservedVolumesDatabaseAccessor(config.me.db.reservedVolumesConnString, tableName)
    }

    @Bean
    open fun azureConfigDatabaseAccessor(@Value("\${azure.config.database.acessor.table}") tableName: String)
            : ConfigDatabaseAccessor {
        return AzureConfigDatabaseAccessor(config.me.db.matchingEngineConnString, tableName)
    }

    @Bean
    @Profile("default")
    open fun azureMonitoringDatabaseAccessor(@Value("\${azure.monitoring.table}") monitoringTable: String,
                                             @Value("\${azure.performance.table}") performanceTable: String)
            : MonitoringDatabaseAccessor {
        return AzureMonitoringDatabaseAccessor(config.me.db.monitoringConnString, monitoringTable, performanceTable)
    }

    @Bean
    open fun azureDictionariesDatabaseAccessor(): DictionariesDatabaseAccessor {
        return AzureDictionariesDatabaseAccessor(config.me.db.dictsConnString)
    }

    @Bean
    open fun fileOrderBookDatabaseAccessor()
            : OrderBookDatabaseAccessor {
        return FileOrderBookDatabaseAccessor(config.me.orderBookPath)
    }

    @Bean
    open fun fileProcessedMessagesDatabaseAccessor()
            : FileProcessedMessagesDatabaseAccessor {
        return FileProcessedMessagesDatabaseAccessor(config.me.processedMessagesPath, config.me.processedMessagesInterval)
    }

    private fun getProcessedMessageTTL(): Int {
        return (config.me.processedMessagesInterval / 500).toInt()
    }

    @Bean
    open fun fileStopOrderBookDatabaseAccessor(): FileStopOrderBookDatabaseAccessor {
        return FileStopOrderBookDatabaseAccessor(config.me.stopOrderBookPath)
    }
}