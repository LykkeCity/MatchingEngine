package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.common.DefaultPersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.monitoring.RedisHealthStatusHolder
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import org.springframework.util.CollectionUtils
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Transaction
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

class RedisPersistenceManager(
        private val primaryBalancesAccessor: RedisWalletDatabaseAccessor,
        private val secondaryBalancesAccessor: WalletDatabaseAccessor?,
        private val redisProcessedMessagesDatabaseAccessor: RedisProcessedMessagesDatabaseAccessor,
        private val redisHealthStatusHolder: RedisHealthStatusHolder,
        private val jedisPool: JedisPool,
        private val config: Config): PersistenceManager {

    companion object {
        private val LOGGER = Logger.getLogger(DefaultPersistenceManager::class.java.name)
        private val REDIS_PERFORMANCE_LOGGER = Logger.getLogger("${DefaultPersistenceManager::class.java.name}.redis")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val updatedWalletsQueue = LinkedBlockingQueue<Collection<Wallet>>()

    init {
        initPersistingIntoSecondaryDb()
    }

    override fun balancesQueueSize() = updatedWalletsQueue.size

    override fun persist(data: PersistenceData): Boolean {
        return try {
            persistData(data)
            true
        } catch (e: Exception) {
            redisHealthStatusHolder.fail()
            val retryMessage = "Unable to save data (${data.details()})"
            LOGGER.error(retryMessage, e)
            METRICS_LOGGER.logError(retryMessage, e)
            false
        }
    }

    private fun persistData(data: PersistenceData) {
        if (isDataEmpty(data)) {
            return
        }

        val startTime = System.nanoTime()

        jedisPool.resource.use { jedis ->
            val transaction = jedis.multi()
            try {
                persistBalances(transaction, data.balancesData?.balances)
                persistProcessedMessages(transaction, data.processedMessage)

                val persistTime = System.nanoTime()

                transaction.exec()
                val commitTime = System.nanoTime()

                REDIS_PERFORMANCE_LOGGER.debug("Total: ${PrintUtils.convertToString2((commitTime - startTime).toDouble())}" +
                        ", persist: ${PrintUtils.convertToString2((persistTime - startTime).toDouble())}" +
                        ", commit: ${PrintUtils.convertToString2((commitTime - persistTime).toDouble())}")

                if (secondaryBalancesAccessor != null && data.balancesData != null) {
                    updatedWalletsQueue.put(data.balancesData.wallets)
                }
            } catch (e: Exception) {
                transaction.clear()
                throw e
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

    private fun persistBalances(transaction: Transaction, assetBalances: Collection<AssetBalance>?) {
        if (CollectionUtils.isEmpty(assetBalances)) {
            return
        }

        LOGGER.trace("Start to persist balances in redis")
        transaction.select(config.me.redis.balanceDatabase)
        primaryBalancesAccessor.insertOrUpdateBalances(transaction, assetBalances!!)
    }

    private fun initPersistingIntoSecondaryDb() {
        if (secondaryBalancesAccessor == null) {
            return
        }

        updatedWalletsQueue.put(primaryBalancesAccessor.loadWallets().values.toList())

        thread(name = "${DefaultPersistenceManager::class.java.name}.asyncWriter") {
            while (true) {
                try {
                    val wallets = updatedWalletsQueue.take()
                    secondaryBalancesAccessor.insertOrUpdateWallets(wallets.toList())
                } catch (e: Exception) {
                    LOGGER.error("Unable to save wallets", e)
                }
            }
        }
    }

    private fun isDataEmpty(data: PersistenceData): Boolean {
        return (data.balancesData == null ||
                CollectionUtils.isEmpty(data.balancesData.wallets) &&
                        CollectionUtils.isEmpty(data.balancesData.balances))
                && data.processedMessage == null
    }

}