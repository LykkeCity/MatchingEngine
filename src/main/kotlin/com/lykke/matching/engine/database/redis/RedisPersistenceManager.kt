
package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.OrderBookPersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.database.redis.accessor.impl.*
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.util.CollectionUtils
import redis.clients.jedis.Transaction

class RedisPersistenceManager(
        private val primaryBalancesAccessor: RedisWalletDatabaseAccessor,
        private val secondaryBalancesAccessor: WalletDatabaseAccessor?,
        private val redisProcessedMessagesDatabaseAccessor: RedisProcessedMessagesDatabaseAccessor,
        private val redisProcessedCashOperationIdDatabaseAccessor: RedisCashOperationIdDatabaseAccessor,
        private val primaryOrdersAccessor: RedisOrderBookDatabaseAccessor,
        private val secondaryOrdersAccessor: OrderBookDatabaseAccessor?,
        private val primaryStopOrdersAccessor: RedisStopOrderBookDatabaseAccessor,
        private val secondaryStopOrdersAccessor: StopOrderBookDatabaseAccessor?,
        private val redisMessageSequenceNumberDatabaseAccessor: RedisMessageSequenceNumberDatabaseAccessor,
        private val persistedWalletsApplicationEventPublisher: SimpleApplicationEventPublisher<Collection<Wallet>>,
        private val redisConnection: RedisConnection,
        private val config: Config): PersistenceManager {

    companion object {
        private val LOGGER = Logger.getLogger(RedisPersistenceManager::class.java.name)
        private val REDIS_PERFORMANCE_LOGGER = Logger.getLogger("${RedisPersistenceManager::class.java.name}.redis")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    override fun persist(data: PersistenceData): Boolean {
        if (data.isEmpty()) {
            return true
        }
        return try {
            persistData(redisConnection, data)
            true
        } catch (e: Exception) {
            val message = "Unable to save data (${data.details()})"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            false
        }
    }

    private fun persistData(redisConnection: RedisConnection, data: PersistenceData) {
        val startTime = System.nanoTime()
        redisConnection.transactionalResource { transaction ->
            persistBalances(transaction, data.balancesData?.balances)
            persistProcessedMessages(transaction, data.processedMessage)

            if (data.processedMessage?.type == MessageType.CASH_IN_OUT_OPERATION.type ||
                    data.processedMessage?.type == MessageType.CASH_TRANSFER_OPERATION.type) {
                persistProcessedCashMessage(transaction, data.processedMessage)
            }

            data.orderBooksData?.let { persistOrders(transaction, it) }
            data.stopOrderBooksData?.let { persistStopOrders(transaction, it) }

            persistMessageSequenceNumber(transaction, data.messageSequenceNumber)

            val persistTime = System.nanoTime()

            transaction.exec()
            val commitTime = System.nanoTime()

            val messageId = data.processedMessage?.messageId
            REDIS_PERFORMANCE_LOGGER.debug("Total: ${PrintUtils.convertToString2((commitTime - startTime).toDouble())}" +
                    ", persist: ${PrintUtils.convertToString2((persistTime - startTime).toDouble())}" +
                    ", commit: ${PrintUtils.convertToString2((commitTime - persistTime).toDouble())}" +
                    (if (messageId != null) " ($messageId)" else ""))

            if (!CollectionUtils.isEmpty(data.balancesData?.wallets)) {
                persistedWalletsApplicationEventPublisher.publishEvent(data.balancesData!!.wallets)
            }

            if (secondaryOrdersAccessor != null && !CollectionUtils.isEmpty(data.orderBooksData?.orderBooks)) {
                updatedOrderBooksQueue.put(data.orderBooksData!!.orderBooks)
            }

            if (secondaryStopOrdersAccessor != null && !CollectionUtils.isEmpty(data.stopOrderBooksData?.orderBooks)) {
                updatedStopOrderBooksQueue.put(data.stopOrderBooksData!!.orderBooks)
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

    private fun persistOrders(transaction: Transaction, data: OrderBooksPersistenceData) {
        if (data.ordersToSave.isEmpty() && data.ordersToRemove.isEmpty()) {
            return
        }
        transaction.select(config.me.redis.ordersDatabase)
        primaryOrdersAccessor.updateOrders(transaction, data.ordersToSave, data.ordersToRemove)
    }

    private fun persistStopOrders(transaction: Transaction, data: OrderBooksPersistenceData) {
        if (data.ordersToSave.isEmpty() && data.ordersToRemove.isEmpty()) {
            return
        }
        transaction.select(config.me.redis.ordersDatabase)
        primaryStopOrdersAccessor.updateOrders(transaction, data.ordersToSave, data.ordersToRemove)
    }

    private fun persistMessageSequenceNumber(transaction: Transaction, sequenceNumber: Long?) {
        if (sequenceNumber == null) {
            return
        }
        redisMessageSequenceNumberDatabaseAccessor.save(transaction, sequenceNumber)
    }
}