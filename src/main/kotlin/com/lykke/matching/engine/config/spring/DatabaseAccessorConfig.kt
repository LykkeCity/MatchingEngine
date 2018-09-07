package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.ConfigDatabaseAccessor
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.ReadOnlyMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.MonitoringDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.ReadOnlyProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.ReservedVolumesDatabaseAccessor
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.azure.AzureBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureConfigDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureHistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureMonitoringDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureReservedVolumesDatabaseAccessor
import com.lykke.matching.engine.database.common.DefaultPersistenceManager
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.connection.impl.RedisReconnectionManager
import com.lykke.matching.engine.database.redis.accessor.impl.RedisCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.database.redis.connection.RedisConnectionFactory
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.TaskScheduler
import java.util.Optional

@Configuration
open class DatabaseAccessorConfig {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var redisConnectionFactory: RedisConnectionFactory

    @Bean
    open fun persistenceManager(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                                ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
                                cashOperationIdDatabaseAccessor: Optional<CashOperationIdDatabaseAccessor>,
                                messageSequenceNumberDatabaseAccessor: Optional<ReadOnlyMessageSequenceNumberDatabaseAccessor>): PersistenceManager {
        return when (config.me.storage) {
            Storage.Azure -> DefaultPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                    ordersDatabaseAccessorsHolder.primaryAccessor,
                    stopOrdersDatabaseAccessorsHolder.primaryAccessor,
                    fileProcessedMessagesDatabaseAccessor())
            Storage.Redis -> {
                RedisPersistenceManager(
                        balancesDatabaseAccessorsHolder.primaryAccessor as RedisWalletDatabaseAccessor,
                        balancesDatabaseAccessorsHolder.secondaryAccessor,
                        redisProcessedMessagesDatabaseAccessor.get(),
                        cashOperationIdDatabaseAccessor.get() as RedisCashOperationIdDatabaseAccessor,
                        ordersDatabaseAccessorsHolder.primaryAccessor as RedisOrderBookDatabaseAccessor,
                        ordersDatabaseAccessorsHolder.secondaryAccessor,
                        stopOrdersDatabaseAccessorsHolder.primaryAccessor as RedisStopOrderBookDatabaseAccessor,
                        stopOrdersDatabaseAccessorsHolder.secondaryAccessor,
                        messageSequenceNumberDatabaseAccessor.get() as RedisMessageSequenceNumberDatabaseAccessor,
                        persistenceRedisConnection()!!,
                        config
                )
            }
        }
    }

    @Bean
    open fun cashInOutOperationPreprocessorPersistenceManager(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                                redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
                                ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                cashOperationIdDatabaseAccessor: Optional<CashOperationIdDatabaseAccessor>,
                                messageSequenceNumberDatabaseAccessor: Optional<ReadOnlyMessageSequenceNumberDatabaseAccessor>): PersistenceManager {
        return when (config.me.storage) {
            Storage.Azure -> DefaultPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                    ordersDatabaseAccessorsHolder.primaryAccessor,
                    stopOrdersDatabaseAccessorsHolder.primaryAccessor,
                    fileProcessedMessagesDatabaseAccessor())
            Storage.Redis -> {
                RedisPersistenceManager(
                        balancesDatabaseAccessorsHolder.primaryAccessor as RedisWalletDatabaseAccessor,
                        balancesDatabaseAccessorsHolder.secondaryAccessor,
                        redisProcessedMessagesDatabaseAccessor.get(),
                        cashOperationIdDatabaseAccessor.get() as RedisCashOperationIdDatabaseAccessor,
                        ordersDatabaseAccessorsHolder.primaryAccessor as RedisOrderBookDatabaseAccessor,
                        ordersDatabaseAccessorsHolder.secondaryAccessor,
                        stopOrdersDatabaseAccessorsHolder.primaryAccessor as RedisStopOrderBookDatabaseAccessor,
                        stopOrdersDatabaseAccessorsHolder.secondaryAccessor,
                        messageSequenceNumberDatabaseAccessor.get() as RedisMessageSequenceNumberDatabaseAccessor,
                        cashInOutOperationsPreprocessorRedisConnection()!!,
                        config
                )
            }
        }
    }

    @Bean
    open fun cashTransferPreprocessorPersistenceManager(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                                                              redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
                                                              ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                                              stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                                              cashOperationIdDatabaseAccessor: Optional<CashOperationIdDatabaseAccessor>,
                                                              messageSequenceNumberDatabaseAccessor: Optional<ReadOnlyMessageSequenceNumberDatabaseAccessor>): PersistenceManager {
        return when (config.me.storage) {
            Storage.Azure -> DefaultPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                    ordersDatabaseAccessorsHolder.primaryAccessor,
                    stopOrdersDatabaseAccessorsHolder.primaryAccessor,
                    fileProcessedMessagesDatabaseAccessor())
            Storage.Redis -> {
                RedisPersistenceManager(
                        balancesDatabaseAccessorsHolder.primaryAccessor as RedisWalletDatabaseAccessor,
                        balancesDatabaseAccessorsHolder.secondaryAccessor,
                        redisProcessedMessagesDatabaseAccessor.get(),
                        cashOperationIdDatabaseAccessor.get() as RedisCashOperationIdDatabaseAccessor,
                        ordersDatabaseAccessorsHolder.primaryAccessor as RedisOrderBookDatabaseAccessor,
                        ordersDatabaseAccessorsHolder.secondaryAccessor,
                        stopOrdersDatabaseAccessorsHolder.primaryAccessor as RedisStopOrderBookDatabaseAccessor,
                        stopOrdersDatabaseAccessorsHolder.secondaryAccessor,
                        messageSequenceNumberDatabaseAccessor.get() as RedisMessageSequenceNumberDatabaseAccessor,
                        cashTransferOperationsPreprocessorRedisConnection()!!,
                        config
                )
            }
        }
    }

    @Bean
    open fun readOnlyProcessedMessagesDatabaseAccessor(): ReadOnlyProcessedMessagesDatabaseAccessor {
        return when (config.me.storage) {
            Storage.Azure -> fileProcessedMessagesDatabaseAccessor()
            Storage.Redis -> RedisProcessedMessagesDatabaseAccessor(initialLoadingRedisConnection()!!,
                    config.me.redis.processedMessageDatabase,
                    getProcessedMessageTTL())
        }
    }

    @Bean
    open fun redisWalletDatabaseAccessor(): RedisWalletDatabaseAccessor? {
        if (config.me.storage != Storage.Redis) {
            return null
        }

        return RedisWalletDatabaseAccessor(initialLoadingRedisConnection()!!, config.me.redis.balanceDatabase)
    }

    @Bean
    open fun redisHolder(taskScheduler: TaskScheduler,
                         applicationEventPublisher: ApplicationEventPublisher,
                         allRedisConnections: List<RedisConnection>,
                         @Value("\${redis.health.check.interval}") updateInterval: Long,
                         @Value("\${redis.health.check.reconnect.interval}") reconnectInterval: Long): RedisReconnectionManager? {
        if (config.me.storage != Storage.Redis) {
            return null
        }

        return RedisReconnectionManager(config.me, allRedisConnections, pingRedisConnection()!!,
                taskScheduler, applicationEventPublisher, updateInterval, reconnectInterval)
    }


    @Bean
    open fun redisProcessedMessagesDatabaseAccessor(): RedisProcessedMessagesDatabaseAccessor? {
        val initialLoadingRedisConnection = initialLoadingRedisConnection() ?: return null
        return RedisProcessedMessagesDatabaseAccessor(initialLoadingRedisConnection,
                config.me.redis.processedMessageDatabase,
                getProcessedMessageTTL())
    }

    @Bean
    open fun cashOperationIdDatabaseAccessor(): CashOperationIdDatabaseAccessor? {
        return when (config.me.storage) {
            Storage.Azure -> AzureCashOperationIdDatabaseAccessor()
            Storage.Redis -> {
                return RedisCashOperationIdDatabaseAccessor(cashInOutOperationIdRedisConnection()!!,
                        cashTransferOperationIdRedisConnection()!!,
                        config.me.redis.processedCashMessageDatabase)
            }
        }
    }

    @Bean
    open fun messageSequenceNumberDatabaseAccessor(): ReadOnlyMessageSequenceNumberDatabaseAccessor {
        return when (config.me.storage) {
            Storage.Azure -> AzureMessageSequenceNumberDatabaseAccessor()
            Storage.Redis -> {
                RedisMessageSequenceNumberDatabaseAccessor(initialLoadingRedisConnection()!!,
                        config.me.redis.sequenceNumberDatabase)
            }
        }
    }

    @Bean
    open fun pingRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("pingRedisConnection")
    }

    @Bean
    open fun cashTransferOperationIdRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashTransferOperationIdRedisConnection")
    }

    @Bean
    open fun cashInOutOperationIdRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashInOutOperationIdRedisConnection")
    }

    @Bean
    open fun initialLoadingRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("initialLoadingRedisConnection")
    }

    @Bean
    open fun persistenceRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("persistenceRedisConnection")
    }

    @Bean
    open fun cashInOutOperationsPreprocessorRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashOperationsPreprocessorRedisConnection")
    }

    @Bean
    open fun cashTransferOperationsPreprocessorRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashTransferOperationsPreprocessorRedisConnection")
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
    open fun fileProcessedMessagesDatabaseAccessor()
            : FileProcessedMessagesDatabaseAccessor {
        return FileProcessedMessagesDatabaseAccessor(config.me.processedMessagesPath, config.me.processedMessagesInterval)
    }

    private fun getProcessedMessageTTL(): Int {
        return (config.me.processedMessagesInterval / 500).toInt()
    }
}