package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.database.redis.accessor.impl.*
import com.lykke.matching.engine.database.reconciliation.events.AccountPersistEvent
import com.lykke.matching.engine.database.redis.accessor.impl.RedisCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.util.CollectionUtils
import redis.clients.jedis.Transaction

class RedisWithoutOrdersPersistenceManager(
        private val primaryBalancesAccessor: RedisWalletDatabaseAccessor,
        private val redisProcessedMessagesDatabaseAccessor: RedisProcessedMessagesDatabaseAccessor,
        private val redisProcessedCashOperationIdDatabaseAccessor: RedisCashOperationIdDatabaseAccessor,
        private val orderBookDatabaseAccessor: OrderBookDatabaseAccessor,
        private val stopOrderBookDatabaseAccessor: StopOrderBookDatabaseAccessor,
        private val redisMessageSequenceNumberDatabaseAccessor: RedisMessageSequenceNumberDatabaseAccessor,
        private val persistedWalletsApplicationEventPublisher: SimpleApplicationEventPublisher<AccountPersistEvent>,
        private val redisMidPriceDatabaseAccessor: RedisMidPriceDatabaseAccessor,
        private val redisConnection: RedisConnection,
        private val config: Config) : PersistenceManager {

    companion object {
        private val LOGGER = Logger.getLogger(RedisWithoutOrdersPersistenceManager::class.java.name)
        private val REDIS_PERFORMANCE_LOGGER = Logger.getLogger("${RedisWithoutOrdersPersistenceManager::class.java.name}.redis")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun persist(data: PersistenceData): Boolean {
        if (data.isEmpty()) {
            return true
        }
        return try {
            if (!data.isEmptyWithoutOrders()) {
                persistRedisData(redisConnection, data)
            }
            if (!data.isOrdersEmpty()) {
                persistOrders(data.orderBooksData)
                persistStopOrders(data.stopOrderBooksData)
            }
            true
        } catch (e: Exception) {
            val message = "Unable to save data (${data.details()})"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            false
        }
    }

    private fun persistRedisData(redisConnection: RedisConnection, data: PersistenceData) {
        val startTime = System.nanoTime()
        redisConnection.transactionalResource { transaction ->
            persistBalances(transaction, data.balancesData?.balances)
            persistProcessedMessages(transaction, data.processedMessage)

            if (data.processedMessage?.type == MessageType.CASH_IN_OUT_OPERATION.type ||
                    data.processedMessage?.type == MessageType.CASH_TRANSFER_OPERATION.type) {
                persistProcessedCashMessage(transaction, data.processedMessage)
            }

            persistMessageSequenceNumber(transaction, data.messageSequenceNumber)
            persistMidPrices(transaction, data.midPricePersistenceData)

            val persistTime = System.nanoTime()

            transaction.exec()
            val commitTime = System.nanoTime()

            val messageId = data.processedMessage?.messageId
            REDIS_PERFORMANCE_LOGGER.debug("Total: ${PrintUtils.convertToString2((commitTime - startTime).toDouble())}" +
                    ", persist: ${PrintUtils.convertToString2((persistTime - startTime).toDouble())}" +
                    ", commit: ${PrintUtils.convertToString2((commitTime - persistTime).toDouble())}" +
                    (if (messageId != null) " ($messageId)" else ""))

            if (!CollectionUtils.isEmpty(data.balancesData?.wallets)) {
                persistedWalletsApplicationEventPublisher.publishEvent(AccountPersistEvent(data.balancesData!!.wallets))
            }
        }
    }

    private fun persistProcessedMessages(transaction: Transaction, processedMessage: ProcessedMessage?) {
        LOGGER.trace("Start to persist processed messages in redis")

        if (processedMessage == null) {
            LOGGER.trace("Processed message is empty, skip persisting")
            return
        }

        redisProcessedMessagesDatabaseAccessor.save(transaction, processedMessage)
    }

    private fun persistProcessedCashMessage(transaction: Transaction, processedMessage: ProcessedMessage) {
        LOGGER.trace("Start to persist processed cash messages in redis")
        redisProcessedCashOperationIdDatabaseAccessor.save(transaction, processedMessage)
    }

    private fun persistBalances(transaction: Transaction, assetBalances: Collection<AssetBalance>?) {
        if (CollectionUtils.isEmpty(assetBalances)) {
            return
        }

        LOGGER.trace("Start to persist balances in redis")
        transaction.select(config.me.redis.balanceDatabase)
        primaryBalancesAccessor.insertOrUpdateBalances(transaction, assetBalances!!)
    }

    private fun persistMessageSequenceNumber(transaction: Transaction, sequenceNumber: Long?) {
        if (sequenceNumber == null) {
            return
        }
        redisMessageSequenceNumberDatabaseAccessor.save(transaction, sequenceNumber)
    }

    private fun persistOrders(data: OrderBooksPersistenceData?) {
        if (data?.isEmpty() != false) {
            return
        }
        data.orderBooks.forEach {
            orderBookDatabaseAccessor.updateOrderBook(it.assetPairId, it.isBuy, it.orders)
        }
    }

    private fun persistStopOrders(data: OrderBooksPersistenceData?) {
        if (data?.isEmpty() != false) {
            return
        }
        data.orderBooks.forEach {
            stopOrderBookDatabaseAccessor.updateStopOrderBook(it.assetPairId, it.isBuy, it.orders)
        }
    }

    private fun persistMidPrices(transaction: Transaction, midPricePersistenceData: MidPricePersistenceData?) {
        LOGGER.trace("Start to persist mid price")

        if (midPricePersistenceData == null) {
            LOGGER.trace("Mid price is empty - skipping")
            return
        }

        if (midPricePersistenceData.removeAll) {
            LOGGER.info("Removing all mid prices")
            redisMidPriceDatabaseAccessor.removeAll(transaction)
            return
        }

        if (midPricePersistenceData.midPrices != null) {
            redisMidPriceDatabaseAccessor.save(transaction, midPricePersistenceData.midPrices)
        }

    }
}