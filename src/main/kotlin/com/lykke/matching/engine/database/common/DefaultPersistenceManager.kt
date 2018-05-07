package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.redis.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

class DefaultPersistenceManager(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder) : PersistenceManager {

    companion object {
        private val LOGGER = Logger.getLogger(DefaultPersistenceManager::class.java.name)
        private val REDIS_PERFORMANCE_LOGGER = Logger.getLogger("${DefaultPersistenceManager::class.java.name}.redis")
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val primaryBalancesAccessor = balancesDatabaseAccessorsHolder.primaryAccessor
    private val secondaryBalancesAccessor = balancesDatabaseAccessorsHolder.secondaryAccessor
    private val isRedisBalancesPrimary = primaryBalancesAccessor is RedisWalletDatabaseAccessor
    private val updatedWalletsQueue = LinkedBlockingQueue<Collection<Wallet>>()
    private val balancesJedisPool = balancesDatabaseAccessorsHolder.jedisPool

    private var time: Long? = null

    override fun balancesQueueSize() = updatedWalletsQueue.size

    override fun persist(data: PersistenceData) {
        try {
            persistData(data)
        } catch (e: Exception) {
            val message = "Unable to save data (${data.details()})"
            LOGGER.error(message, e)
            METRICS_LOGGER.logError(message, e)
        }
    }

    private fun fixPerformanceTime(message: String) {
        REDIS_PERFORMANCE_LOGGER.debug("$message: ${PrintUtils.convertToString2((System.nanoTime() - time!!).toDouble())}")
        time = System.nanoTime()
    }

    private fun persistData(data: PersistenceData) {
        if (!isRedisBalancesPrimary) {
            primaryBalancesAccessor.insertOrUpdateWallets(data.wallets.toList())
            return
        }

        time = System.nanoTime()
        val details = data.details()
        balancesJedisPool!!.resource.use { balancesJedis ->
            fixPerformanceTime("Resource")
            val transaction = balancesJedis!!.multi()
            var success = false
            try {
                primaryBalancesAccessor as RedisWalletDatabaseAccessor
                primaryBalancesAccessor.insertOrUpdateBalances(transaction, data.balances)
                fixPerformanceTime("Persist ($details)")
                success = true
            } finally {
                if (success) {
                    transaction.exec()
                    fixPerformanceTime("Commit")
                } else {
                    balancesJedis.resetState()
                    fixPerformanceTime("Reset")
                }
            }
        }
        if (secondaryBalancesAccessor != null) {
            updatedWalletsQueue.put(data.wallets)
        }
    }

    private fun init() {
        if (!isRedisBalancesPrimary || secondaryBalancesAccessor == null) {
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