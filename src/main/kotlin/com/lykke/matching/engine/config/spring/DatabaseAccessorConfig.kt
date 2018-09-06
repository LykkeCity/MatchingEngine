package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.azure.*
import com.lykke.matching.engine.database.common.DefaultPersistenceManager
import com.lykke.matching.engine.database.common.PersistenceManagerFactory
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.accessor.impl.*
import com.lykke.matching.engine.database.redis.connection.impl.RedisReconnectionManager
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

    @Autowired
    private lateinit var persistenceManagerFactory: PersistenceManagerFactory

    @Bean
    open fun persistenceManager(): PersistenceManager {
        return persistenceManagerFactory.get("persistenceRedisConnection")
    }

    @Bean
    open fun cashInOutOperationPreprocessorPersistenceManager(): PersistenceManager {
        return persistenceManagerFactory.get("cashInOutOperationPreprocessorRedisConnection")
    }


    @Bean
    open fun limitOrderCancelOperationPreprocessorPersistenceManager(): PersistenceManager {
        return persistenceManagerFactory.get("limitOrderCancelOperationPreprocessorRedisConnection")

    }

    @Bean
    open fun cashTransferOperationPreprocessorPersistenceManager(): PersistenceManager {
        return persistenceManagerFactory.get("cashTransferOperationsPreprocessorRedisConnection")
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
    open fun backOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
        return AzureBackOfficeDatabaseAccessor(config.me.db.dictsConnString)
    }

    @Bean
    open fun azureCashOperationsDatabaseAccessor(@Value("\${azure.cache.operation.table}") tableName: String)
            : CashOperationsDatabaseAccessor {
        return AzureCashOperationsDatabaseAccessor(config.me.db.balancesInfoConnString, tableName)
    }

    @Bean
    open fun azureHistoryTicksDatabaseAccessor(@Value("\${application.tick.frequency}") frequency: Long)
            : HistoryTicksDatabaseAccessor {
        return AzureHistoryTicksDatabaseAccessor(config.me.db.hLiquidityConnString, frequency)
    }

    @Bean
    open fun azureLimitOrderDatabaseAccessor(@Value("\${azure.best.price.table}") bestPricesTable: String,
                                             @Value("\${azure.candles.table}") candlesTable: String,
                                             @Value("\${azure.hour.candles.table}") hourCandlesTable: String)
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
    open fun azureSettingsDatabaseAccessor(@Value("\${azure.settings.database.accessor.table}") tableName: String)
            : SettingsDatabaseAccessor {
        return AzureSettingsDatabaseAccessor(config.me.db.matchingEngineConnString, tableName)
    }

    @Bean
    open fun aettingsHistoryDatabaseAccessor(@Value("\${azure.settings.history.database.accessor.table}") tableName: String): SettingsHistoryDatabaseAccessor {
        return AzureSettingsHistoryDatabaseAccessor(config.me.db.matchingEngineConnString, tableName)
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