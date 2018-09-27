package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.ReadOnlyMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.accessor.impl.*
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.BlockingQueue

@Component
class PersistenceManagerFactoryImpl(private val balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                                    private val ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                    private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                    private val redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
                                    private val cashOperationIdDatabaseAccessor: Optional<CashOperationIdDatabaseAccessor>,
                                    private val messageSequenceNumberDatabaseAccessor: Optional<ReadOnlyMessageSequenceNumberDatabaseAccessor>,
                                    private val fileProcessedMessagesDatabaseAccessor: FileProcessedMessagesDatabaseAccessor,
                                    private val updatedWalletsQueue: BlockingQueue<Collection<Wallet>>,
                                    private val updatedOrderBooksQueue: BlockingQueue<Collection<OrderBookPersistenceData>>,
                                    private val updatedStopOrderBooksQueue: BlockingQueue<Collection<OrderBookPersistenceData>>,
                                    private val config: Config) : PersistenceManagerFactory {

    override fun get(redisConnection: Optional<RedisConnection>): PersistenceManager {
        return when (config.me.storage) {
            Storage.Azure -> DefaultPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                    ordersDatabaseAccessorsHolder.primaryAccessor,
                    stopOrdersDatabaseAccessorsHolder.primaryAccessor,
                    fileProcessedMessagesDatabaseAccessor)
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
                        updatedWalletsQueue,
                        updatedOrderBooksQueue,
                        updatedStopOrderBooksQueue,
                        redisConnection.get(),
                        config
                )
            }
        }
    }
}