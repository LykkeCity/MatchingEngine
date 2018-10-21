package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.azure.*
import com.lykke.matching.engine.database.common.PersistenceManagerFactory
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.util.*

@Configuration
open class DatabaseAccessorConfig {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var persistenceManagerFactory: PersistenceManagerFactory

    //<editor-fold desc="Persistence managers">
    @Bean
    open fun persistenceManager(persistenceRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(persistenceRedisConnection)
    }

    @Bean
    open fun cashInOutOperationPreprocessorPersistenceManager(cashInOutOperationPreprocessorRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(cashInOutOperationPreprocessorRedisConnection)
    }


    @Bean
    open fun limitOrderCancelOperationPreprocessorPersistenceManager(limitOrderCancelOperationPreprocessorRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(limitOrderCancelOperationPreprocessorRedisConnection)

    }

    @Bean
    open fun cashTransferPreprocessorPersistenceManager(cashTransferOperationsPreprocessorRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(cashTransferOperationsPreprocessorRedisConnection)
    }
    //</editor-fold>

    //<editor-fold desc="Multisource database accessors">
    @Bean
    open fun readOnlyProcessedMessagesDatabaseAccessor(redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>): ReadOnlyProcessedMessagesDatabaseAccessor {
        return when (config.me.storage) {
            Storage.Azure -> fileProcessedMessagesDatabaseAccessor()
            Storage.RedisWithoutOrders,
            Storage.Redis -> redisProcessedMessagesDatabaseAccessor.get()
        }
    }

    @Bean
    open fun cashOperationIdDatabaseAccessor(redisCashOperationIdDatabaseAccessor: Optional<RedisCashOperationIdDatabaseAccessor>): CashOperationIdDatabaseAccessor? {
        return when (config.me.storage) {
            Storage.Azure -> AzureCashOperationIdDatabaseAccessor()
            Storage.RedisWithoutOrders,
            Storage.Redis -> redisCashOperationIdDatabaseAccessor.get()
        }
    }

    @Bean
    open fun messageSequenceNumberDatabaseAccessor(redisMessageSequenceNumberDatabaseAccessor: Optional<RedisMessageSequenceNumberDatabaseAccessor>): ReadOnlyMessageSequenceNumberDatabaseAccessor {
        return when (config.me.storage) {
            Storage.Azure -> AzureMessageSequenceNumberDatabaseAccessor()
            Storage.RedisWithoutOrders,
            Storage.Redis -> redisMessageSequenceNumberDatabaseAccessor.get()
        }
    }
    //</editor-fold>

    //<editor-fold desc="Azure DB accessors">
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
    open fun settingsHistoryDatabaseAccessor(@Value("\${azure.settings.history.database.accessor.table}") tableName: String): SettingsHistoryDatabaseAccessor {
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
    //</editor-fold>

    //<editor-fold desc="File db accessors">
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

    @Bean
    open fun fileStopOrderBookDatabaseAccessor(): FileStopOrderBookDatabaseAccessor {
        return FileStopOrderBookDatabaseAccessor(config.me.stopOrderBookPath)
    }
    //</editor-fold>
}