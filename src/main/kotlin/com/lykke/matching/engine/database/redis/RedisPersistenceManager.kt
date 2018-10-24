package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.OrderBookDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.StopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.common.entity.MidPricePersistenceData
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
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

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
        private val redisMidPriceDatabaseAccessor: RedisMidPriceDatabaseAccessor,
        private val redisConnection: RedisConnection,
        private val config: Config): PersistenceManager {

    companion object {
        private val LOGGER = Logger.getLogger(RedisPersistenceManager::class.java.name)
        private val REDIS_PERFORMANCE_LOGGER = Logger.getLogger("${RedisPersistenceManager::class.java.name}.redis")
        private val METRICS_LOGGER = MetricsLogger.getLogger()

        fun mapOrdersToOrderBookPersistenceDataList(orders: Collection<LimitOrder>) = mapOrdersToOrderBookPersistenceDataList(orders, emptyList())

        private fun mapOrdersToOrderBookPersistenceDataList(orders: Collection<LimitOrder>, orderBooksSides: Collection<OrderBookSide>): List<OrderBookPersistenceData> {
            val orderBooks = mutableMapOf<String, MutableMap<Boolean, MutableCollection<LimitOrder>>>()
            orders.forEach { order ->
                orderBooks.getOrPut(order.assetPairId) { mutableMapOf() }
                        .getOrPut(order.isBuySide()) { mutableListOf() }
                        .add(order)
            }

            val mutableOrderBooksSides = orderBooksSides.toMutableList()
            val orderBookPersistenceDataList = mutableListOf<OrderBookPersistenceData>()
            orderBooks.forEach {assetPairId, sideOrders ->
                sideOrders.forEach { isBuy, orders ->
                    mutableOrderBooksSides.remove(OrderBookSide(assetPairId, isBuy))
                    orderBookPersistenceDataList.add(OrderBookPersistenceData(assetPairId, isBuy, orders))
                }
            }
            mutableOrderBooksSides.forEach { orderBooksSide ->
                LOGGER.info("Orders $orderBooksSide are absent in primary db and will be removed from secondary db")
                orderBookPersistenceDataList.add(OrderBookPersistenceData(orderBooksSide.assetPairId, orderBooksSide.isBuySide, emptyList()))
            }
            return orderBookPersistenceDataList
        }
    }

    private val updatedWalletsQueue = LinkedBlockingQueue<Collection<Wallet>>()
    private val updatedOrderBooksQueue = LinkedBlockingQueue<Collection<OrderBookPersistenceData>>()
    private val updatedStopOrderBooksQueue = LinkedBlockingQueue<Collection<OrderBookPersistenceData>>()

    init {
        startSecondaryBalancesUpdater()
        startSecondaryOrdersUpdater()
        startSecondaryStopOrdersUpdater()
    }

    override fun balancesQueueSize() = updatedWalletsQueue.size

    override fun ordersQueueSize() = updatedOrderBooksQueue.size

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
            persistMidPrices(transaction, data.midPricePersistenceData)

            val persistTime = System.nanoTime()

            transaction.exec()
            val commitTime = System.nanoTime()

            val messageId = data.processedMessage?.messageId
            REDIS_PERFORMANCE_LOGGER.debug("Total: ${PrintUtils.convertToString2((commitTime - startTime).toDouble())}" +
                    ", persist: ${PrintUtils.convertToString2((persistTime - startTime).toDouble())}" +
                    ", commit: ${PrintUtils.convertToString2((commitTime - persistTime).toDouble())}" +
                    (if (messageId != null) " ($messageId)" else ""))

            if (secondaryBalancesAccessor != null && !CollectionUtils.isEmpty(data.balancesData?.wallets)) {
                updatedWalletsQueue.put(data.balancesData!!.wallets)
            }

            if (secondaryOrdersAccessor != null && !CollectionUtils.isEmpty(data.orderBooksData?.orderBooks)) {
                updatedOrderBooksQueue.put(data.orderBooksData!!.orderBooks)
            }

            if (secondaryStopOrdersAccessor != null && !CollectionUtils.isEmpty(data.stopOrderBooksData?.orderBooks)) {
                updatedStopOrderBooksQueue.put(data.stopOrderBooksData!!.orderBooks)
            }
        }
    }

    private fun persistMidPrices(transaction: Transaction, midPricePersistenceData: MidPricePersistenceData?) {
        LOGGER.trace("Start to persist mid price")

        if (midPricePersistenceData == null) {
            LOGGER.trace("Mid price is empty - skipping")
            return
        }

        if (midPricePersistenceData.removeAll) {
            LOGGER.info("Remove all mid prices")
            redisMidPriceDatabaseAccessor.removeAll(transaction)
            return
        }

        if (midPricePersistenceData.midPrices != null) {
            redisMidPriceDatabaseAccessor.save(transaction, midPricePersistenceData.midPrices)
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

    private fun startSecondaryBalancesUpdater() {
        if (secondaryBalancesAccessor == null) {
            return
        }

        if (!config.me.walletsMigration) {
            updatedWalletsQueue.put(primaryBalancesAccessor.loadWallets().values.toList())
        }

        thread(name = "${RedisPersistenceManager::class.java.name}.balancesAsyncWriter") {
            while (true) {
                try {
                    val wallets = updatedWalletsQueue.take()
                    secondaryBalancesAccessor.insertOrUpdateWallets(wallets.toList())
                } catch (e: Exception) {
                    LOGGER.error("Unable to save wallets async", e)
                }
            }
        }
    }

    private fun startSecondaryOrdersUpdater() {
        if (secondaryOrdersAccessor == null) {
            return
        }

        if (!config.me.ordersMigration) {
            val currentOrderBookSides = secondaryOrdersAccessor.loadLimitOrders().map { OrderBookSide(it.assetPairId, it.isBuySide()) }.toSet()
            updatedOrderBooksQueue.put(mapOrdersToOrderBookPersistenceDataList(primaryOrdersAccessor.loadLimitOrders(), currentOrderBookSides))
        }

        thread(name = "${RedisPersistenceManager::class.java.name}.ordersAsyncWriter") {
            while (true) {
                try {
                    val orderBooks = updatedOrderBooksQueue.take()
                    orderBooks.forEach {
                        secondaryOrdersAccessor.updateOrderBook(it.assetPairId, it.isBuy, it.orders)
                    }
                } catch (e: Exception) {
                    LOGGER.error("Unable to save orders async", e)
                }
            }
        }
    }

    private fun startSecondaryStopOrdersUpdater() {
        if (secondaryStopOrdersAccessor == null) {
            return
        }

        if (!config.me.ordersMigration) {
            val currentStopOrderBookSides = secondaryStopOrdersAccessor.loadStopLimitOrders().map { OrderBookSide(it.assetPairId, it.isBuySide()) }.toSet()
            updatedStopOrderBooksQueue.put(mapOrdersToOrderBookPersistenceDataList(primaryStopOrdersAccessor.loadStopLimitOrders(), currentStopOrderBookSides))
        }

        thread(name = "${RedisPersistenceManager::class.java.name}.stopOrdersAsyncWriter") {
            while (true) {
                try {
                    val orderBooks = updatedStopOrderBooksQueue.take()
                    orderBooks.forEach {
                        secondaryStopOrdersAccessor.updateStopOrderBook(it.assetPairId, it.isBuy, it.orders)
                    }
                } catch (e: Exception) {
                    LOGGER.error("Unable to save stop orders async", e)
                }
            }
        }
    }

}

private data class OrderBookSide(val assetPairId: String,
                                 val isBuySide: Boolean)