package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.database.redis.accessor.impl.*
import com.lykke.matching.engine.database.common.strategy.OrdersPersistInSecondaryDbStrategy
import com.lykke.matching.engine.database.common.strategy.PersistOrdersDuringRedisTransactionStrategy
import com.lykke.matching.engine.database.reconciliation.events.AccountPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.MidPricesPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.OrderBookPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.StopOrderBookPersistEvent
import com.lykke.matching.engine.database.redis.accessor.impl.*
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.CurrentTransactionDataHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.util.CollectionUtils
import redis.clients.jedis.Transaction
import redis.clients.jedis.exceptions.JedisException

class RedisPersistenceManager(
        private val primaryBalancesAccessor: RedisWalletDatabaseAccessor,
        private val redisProcessedMessagesDatabaseAccessor: RedisProcessedMessagesDatabaseAccessor,
        private val redisProcessedCashOperationIdDatabaseAccessor: RedisCashOperationIdDatabaseAccessor,
        private val persistOrdersStrategy: PersistOrdersDuringRedisTransactionStrategy,
        private val ordersPersistInSecondaryDbStrategy: OrdersPersistInSecondaryDbStrategy?,
        private val redisMessageSequenceNumberDatabaseAccessor: RedisMessageSequenceNumberDatabaseAccessor,
        private val persistedWalletsApplicationEventPublisher: SimpleApplicationEventPublisher<AccountPersistEvent>,
        private val persistMidPricesApplicationEventPublisher: SimpleApplicationEventPublisher<MidPricesPersistEvent>,
        private val redisConnection: RedisConnection,
        private val config: Config,
        private val currentTransactionDataHolder: CurrentTransactionDataHolder,
        private val performanceStatsHolder: PerformanceStatsHolder) : PersistenceManager {

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
            val startTime = System.nanoTime()
            redisConnection.transactionalResource { transaction ->
                persistData(transaction, data, startTime)
            }
            true
        } catch (e: Exception) {
            val message = "Unable to save data (${data.getSummary()})"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
            false
        }
    }

    private fun persistData(transaction: Transaction, data: PersistenceData, startTime: Long) {
        persistBalances(transaction, data.balancesData?.balances)
        persistProcessedMessages(transaction, data.processedMessage)

        if (data.processedMessage?.type == MessageType.CASH_IN_OUT_OPERATION.type ||
                data.processedMessage?.type == MessageType.CASH_TRANSFER_OPERATION.type) {
            persistProcessedCashMessage(transaction, data.processedMessage)
        }

        val startPersistOrders = System.nanoTime()
        persistOrders(transaction, data)
        val endPersistOrders = System.nanoTime()

        persistMessageSequenceNumber(transaction, data.messageSequenceNumber)
        persistMidPrices(data.midPricePersistenceData)

        val persistTime = System.nanoTime()

        transaction.exec()
        val commitTime = System.nanoTime()
        val nonRedisOrdersPersistTime = if (persistOrdersStrategy.isRedisTransactionUsed()) 0 else endPersistOrders - startPersistOrders
        val messageId = data.processedMessage?.messageId
        REDIS_PERFORMANCE_LOGGER.debug("Total: ${PrintUtils.convertToString2((commitTime - startTime - nonRedisOrdersPersistTime).toDouble())}" +
                ", persist: ${PrintUtils.convertToString2((persistTime - startTime - nonRedisOrdersPersistTime).toDouble())}" +
                (if (nonRedisOrdersPersistTime != 0L) ", non redis orders persist time: ${PrintUtils.convertToString2(nonRedisOrdersPersistTime.toDouble())}" else "") +
                ", commit: ${PrintUtils.convertToString2((commitTime - persistTime).toDouble())}" +
                ", persisted data summary: ${data.getSummary()}" +
                (if (messageId != null) ", messageId: ($messageId)" else ""))

        currentTransactionDataHolder.getMessageType()?.let {
            performanceStatsHolder.addPersistTime(it.type, commitTime - startTime)
        }

        if (!CollectionUtils.isEmpty(data.balancesData?.wallets)) {
            persistedWalletsApplicationEventPublisher.publishEvent(AccountPersistEvent(data.balancesData!!.wallets))
        }

        ordersPersistInSecondaryDbStrategy?.persistOrders(data.orderBooksData, data.stopOrderBooksData)
    }


    private fun persistMidPrices(midPricePersistenceData: MidPricePersistenceData?) {
        if (midPricePersistenceData == null) {
            RedisPersistenceManager.LOGGER.trace("Mid price is empty - skipping")
            return
        }
        persistMidPricesApplicationEventPublisher.publishEvent(MidPricesPersistEvent(midPricePersistenceData))
    }

    private fun persistOrders(transaction: Transaction, data: PersistenceData) {
        try {
            persistOrdersStrategy.persist(transaction, data.orderBooksData, data.stopOrderBooksData)
        } catch (e: JedisException) {
            throw e
        } catch (e: Exception) {
            transaction.discard()
            throw e
        }
        persistMidPricesApplicationEventPublisher.publishEvent(MidPricesPersistEvent(midPricePersistenceData))
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
}