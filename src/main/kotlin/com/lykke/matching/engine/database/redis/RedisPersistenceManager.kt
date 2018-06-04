package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.common.DefaultPersistenceManager
import com.lykke.matching.engine.database.common.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

class RedisPersistenceManager(
        private val primaryBalancesAccessor: RedisWalletDatabaseAccessor,
        private val secondaryBalancesAccessor: WalletDatabaseAccessor?,
        private val jedisHolder: DefaultJedisHolder): PersistenceManager {

    companion object {
        private val LOGGER = Logger.getLogger(DefaultPersistenceManager::class.java.name)
        private val REDIS_PERFORMANCE_LOGGER = Logger.getLogger("${DefaultPersistenceManager::class.java.name}.redis")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val updatedWalletsQueue = LinkedBlockingQueue<Collection<Wallet>>()

    override fun balancesQueueSize() = updatedWalletsQueue.size

    override fun persist(data: PersistenceData): Boolean {
        return try {
            persistData(data)
            true
        } catch (e: Exception) {
            jedisHolder.fail()
            val retryMessage = "Unable to save data (${data.details()})"
            LOGGER.error(retryMessage, e)
            METRICS_LOGGER.logError(retryMessage, e)
            false
        }
    }

    private fun persistData(data: PersistenceData) {
        val startTime = System.nanoTime()

        jedisHolder.resource().use { jedis ->
            val transaction = jedis.multi()
            try {
                transaction.select(jedisHolder.balanceDatabase())
                primaryBalancesAccessor.insertOrUpdateBalances(transaction, data.balances)
                val persistTime = System.nanoTime()

                transaction.exec()
                val commitTime = System.nanoTime()

                REDIS_PERFORMANCE_LOGGER.debug("Total: ${PrintUtils.convertToString2((commitTime - startTime).toDouble())}" +
                        ", persist: ${PrintUtils.convertToString2((persistTime - startTime).toDouble())}" +
                        ", commit: ${PrintUtils.convertToString2((commitTime - persistTime).toDouble())}")

                if (secondaryBalancesAccessor != null) {
                    updatedWalletsQueue.put(data.wallets)
                }
            } catch (e: Exception) {
                transaction.clear()
                throw e
            }
        }
    }

    private fun persistProcessedMessages(transaction: Transaction, processedMessage: ProcessedMessage?) {
        LOGGER.info("Start to persist processed messages in redis")
        transaction.select(redisConfig.processedMessageDatabase)

        if (processedMessage == null) {
            LOGGER.debug("Processed message is empty, skip persisting")
            return
        }

        redisProcessedMessagesDatabaseAccessor.save(transaction, processedMessage)
    }

    private fun persistBalances(transaction: Transaction, assetBalances: Collection<AssetBalance>) {
        LOGGER.info("Start to persist balances in redis")
        transaction.select(redisConfig.balanceDatabase)
        primaryBalancesAccessor.insertOrUpdateBalances(transaction, assetBalances)
    }

    private fun init() {
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

    init {
        init()
    }
}