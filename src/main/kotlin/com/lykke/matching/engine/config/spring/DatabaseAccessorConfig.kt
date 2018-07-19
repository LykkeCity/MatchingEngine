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
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
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
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.CashOperationIdRedisHolder
import com.lykke.matching.engine.database.redis.InitialLoadingRedisHolder
import com.lykke.matching.engine.database.redis.PersistenceRedisHolder
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.accessor.impl.RedisCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.util.Optional

@Configuration
open class DatabaseAccessorConfig {

    @Autowired
    private lateinit var config: Config

    @Bean
    open fun persistenceManager(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                                redisHolder: Optional<PersistenceRedisHolder>,
                                redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
                                cashOperationIdDatabaseAccessor: Optional<CashOperationIdDatabaseAccessor>,
                                messageSequenceNumberDatabaseAccessor: Optional<ReadOnlyMessageSequenceNumberDatabaseAccessor>): PersistenceManager {
        return when (config.me.storage) {
            Storage.Azure -> DefaultPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor, fileProcessedMessagesDatabaseAccessor())
            Storage.Redis -> {
                RedisPersistenceManager(
                        balancesDatabaseAccessorsHolder.primaryAccessor as RedisWalletDatabaseAccessor,
                        balancesDatabaseAccessorsHolder.secondaryAccessor,
                        redisProcessedMessagesDatabaseAccessor.get(),
                        cashOperationIdDatabaseAccessor.get() as RedisCashOperationIdDatabaseAccessor,
                        messageSequenceNumberDatabaseAccessor.get() as RedisMessageSequenceNumberDatabaseAccessor,
                        redisHolder.get(),
                        config
                )
            }
        }
    }

    @Bean
    open fun readOnlyProcessedMessagesDatabaseAccessor(redisHolder: Optional<InitialLoadingRedisHolder>): ReadOnlyProcessedMessagesDatabaseAccessor {
        return when (config.me.storage) {
            Storage.Azure -> fileProcessedMessagesDatabaseAccessor()
            Storage.Redis -> RedisProcessedMessagesDatabaseAccessor(redisHolder.get(),
                    config.me.redis.processedMessageDatabase,
                    getProcessedMessageTTL())
        }
    }

    @Bean
    open fun redisProcessedMessagesDatabaseAccessor(redisHolder: Optional<InitialLoadingRedisHolder>): RedisProcessedMessagesDatabaseAccessor? {
        if (!redisHolder.isPresent) {
            return null
        }
        return RedisProcessedMessagesDatabaseAccessor(redisHolder.get(),
                config.me.redis.processedMessageDatabase,
                getProcessedMessageTTL())
    }

    @Bean
    open fun cashOperationIdDatabaseAccessor(redisHolder: Optional<CashOperationIdRedisHolder>): CashOperationIdDatabaseAccessor? {
        return when (config.me.storage) {
            Storage.Azure -> AzureCashOperationIdDatabaseAccessor()
            Storage.Redis -> {
                if (!redisHolder.isPresent) {
                    return null
                }
                return RedisCashOperationIdDatabaseAccessor(redisHolder.get(),
                        config.me.redis.processedCashMessageDatabase)
            }
        }
    }

    @Bean
    open fun messageSequenceNumberDatabaseAccessor(redisHolder: Optional<InitialLoadingRedisHolder>): ReadOnlyMessageSequenceNumberDatabaseAccessor {
        return when (config.me.storage) {
            Storage.Azure -> AzureMessageSequenceNumberDatabaseAccessor()
            Storage.Redis -> {
                RedisMessageSequenceNumberDatabaseAccessor(redisHolder.get(),
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