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

    private fun fixTime(): String {
        val result = System.nanoTime() - time!!
        time = System.nanoTime()
        return PrintUtils.convertToString2(result.toDouble())
    }

    private fun persistData(data: PersistenceData) {
        if (!isRedisBalancesPrimary) {
            primaryBalancesAccessor.insertOrUpdateWallets(data.wallets.toList())
            return
        }

        time = System.nanoTime()
        var resourceTime: String? = null
        var persistenceTime: String? = null
        var commitTime: String? = null
        balancesJedisPool!!.resource.use { balancesJedis ->
            resourceTime = fixTime()
            val pipeline = balancesJedis.pipelined()
            pipeline.multi()

            primaryBalancesAccessor as RedisWalletDatabaseAccessor
            primaryBalancesAccessor.insertOrUpdateBalances(pipeline, data.balances)
            persistenceTime = fixTime()

            pipeline.exec()
            commitTime = fixTime()
        }
        val closeTime = fixTime()

        REDIS_PERFORMANCE_LOGGER.debug("Resource: $resourceTime" +
                ", persist: $persistenceTime" +
                ", commit: $commitTime" +
                ", closeTime: $closeTime (${data.details()})")
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