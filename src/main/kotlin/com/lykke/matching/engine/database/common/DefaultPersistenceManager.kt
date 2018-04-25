package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.redis.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

class DefaultPersistenceManager(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder) : PersistenceManager {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(DefaultPersistenceManager::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val primaryBalancesAccessor = balancesDatabaseAccessorsHolder.primaryAccessor
    private val secondaryBalancesAccessor = balancesDatabaseAccessorsHolder.secondaryAccessor
    private val balancesJedisPoolHolder = balancesDatabaseAccessorsHolder.jedisPoolHolder
    private val isRedisBalancesPrimary = primaryBalancesAccessor is RedisWalletDatabaseAccessor
    private val updatedWalletsQueue = LinkedBlockingQueue<Collection<Wallet>>()

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

    private fun persistData(data: PersistenceData) {
        if (!isRedisBalancesPrimary) {
            primaryBalancesAccessor.insertOrUpdateWallets(data.wallets.toList())
            return
        }

        val balancesJedis = balancesJedisPoolHolder!!.resource()
        balancesJedis.use {
            val transaction = balancesJedis.multi()
            var success = false
            try {
                primaryBalancesAccessor as RedisWalletDatabaseAccessor
                primaryBalancesAccessor.insertOrUpdateBalances(transaction, data.balances)
                if (secondaryBalancesAccessor != null) {
                    updatedWalletsQueue.put(data.wallets)
                }
                success = true
            } finally {
                if (success) transaction.exec() else balancesJedis.resetState()
            }
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