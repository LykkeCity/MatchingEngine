package com.lykke.matching.engine.database.redis

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.common.DefaultPersistenceManager
import com.lykke.matching.engine.database.common.PersistenceData
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.config.RedisConfig
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import redis.clients.jedis.Jedis
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

class RedisPersistenceManager(
        private val primaryBalancesAccessor: RedisWalletDatabaseAccessor,
        private val secondaryBalancesAccessor: WalletDatabaseAccessor?,
        private val redisConfig: RedisConfig): PersistenceManager {

    companion object {
        private val LOGGER = Logger.getLogger(DefaultPersistenceManager::class.java.name)
        private val REDIS_PERFORMANCE_LOGGER = Logger.getLogger("${DefaultPersistenceManager::class.java.name}.redis")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val updatedWalletsQueue = LinkedBlockingQueue<Collection<Wallet>>()
    private var jedis = openRedisConnection()

    override fun balancesQueueSize() = updatedWalletsQueue.size

    override fun persist(data: PersistenceData): Boolean {
        return try {
            persistData(data)
            true
        } catch (e: Exception) {
            val retryMessage = "Unable to save data (${data.details()}), retrying"
            LOGGER.error(retryMessage, e)
            METRICS_LOGGER.logError(retryMessage, e)

            return try {
                reinit()
                persistData(data)
                true
            } catch (e: Exception) {
                val message = "Unable to save data (${data.details()})"
                LOGGER.error(message, e)
                METRICS_LOGGER.logError(message, e)
                false
            }
        }
    }

    private fun persistData(data: PersistenceData) {
        val startTime = System.nanoTime()

        val transaction = jedis.multi()
        try {
            transaction.select(redisConfig.balanceDatabase)
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

    private fun reinit(){
        jedis = openRedisConnection()
    }

    private fun openRedisConnection(): Jedis {
        val jedis = Jedis(redisConfig.host, redisConfig.port, redisConfig.timeout, redisConfig.useSsl)
        jedis.connect()
        if (redisConfig.password != null) {
            jedis.auth(redisConfig.password)
        }
        return jedis
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
                    if (wallets.size == 1) {
                        secondaryBalancesAccessor.insertOrUpdateWallet(wallets.first())
                    } else {
                        secondaryBalancesAccessor.insertOrUpdateWallets(wallets.toList())
                    }
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