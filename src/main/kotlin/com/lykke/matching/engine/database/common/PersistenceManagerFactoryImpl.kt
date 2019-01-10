package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.ReadOnlyMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.common.strategy.OrdersPersistInSecondaryDbStrategy
import com.lykke.matching.engine.database.common.strategy.PersistOrdersDuringRedisTransactionStrategy
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.reconciliation.events.AccountPersistEvent
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.accessor.impl.*
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.CurrentTransactionDataHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.stereotype.Component
import java.util.*

@Component
class PersistenceManagerFactoryImpl(private val balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                                    private val ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                    private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                    private val redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
                                    private val cashOperationIdDatabaseAccessor: Optional<CashOperationIdDatabaseAccessor>,
                                    private val messageSequenceNumberDatabaseAccessor: Optional<ReadOnlyMessageSequenceNumberDatabaseAccessor>,
                                    private val fileProcessedMessagesDatabaseAccessor: FileProcessedMessagesDatabaseAccessor,
                                    private val persistedWalletsApplicationEventPublisher: SimpleApplicationEventPublisher<AccountPersistEvent>,
                                    private val config: Config,
                                    private val currentTransactionDataHolder: CurrentTransactionDataHolder,
                                    private val performanceStatsHolder: PerformanceStatsHolder,
                                    private val persistOrdersStrategy: Optional<PersistOrdersDuringRedisTransactionStrategy>,
                                    private val ordersPersistInSecondaryDbStrategy: Optional<OrdersPersistInSecondaryDbStrategy>) : PersistenceManagerFactory {

    override fun get(redisConnection: Optional<RedisConnection>): PersistenceManager {
        return when (config.me.storage) {
            Storage.Azure -> DefaultPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                    ordersDatabaseAccessorsHolder.primaryAccessor,
                    stopOrdersDatabaseAccessorsHolder.primaryAccessor,
                    fileProcessedMessagesDatabaseAccessor)
            Storage.Redis, Storage.RedisWithoutOrders -> {
                RedisPersistenceManager(
                        balancesDatabaseAccessorsHolder.primaryAccessor as RedisWalletDatabaseAccessor,
                        redisProcessedMessagesDatabaseAccessor.get(),
                        cashOperationIdDatabaseAccessor.get() as RedisCashOperationIdDatabaseAccessor,
                        persistOrdersStrategy.get(),
                        ordersPersistInSecondaryDbStrategy.get(),
                        messageSequenceNumberDatabaseAccessor.get() as RedisMessageSequenceNumberDatabaseAccessor,
                        persistedWalletsApplicationEventPublisher,
                        redisConnection.get(),
                        config,
                        currentTransactionDataHolder,
                        performanceStatsHolder
                )
            }
        }
    }
}