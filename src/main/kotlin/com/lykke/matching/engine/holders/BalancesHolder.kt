package com.lykke.matching.engine.holders

import com.lykke.matching.engine.balance.WalletOperationsProcessor
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.common.PersistenceData
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.updaters.BalancesUpdater
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.concurrent.BlockingQueue

@Component
class BalancesHolder(private val balancesDbAccessorsHolder: BalancesDatabaseAccessorsHolder,
                     private val persistenceManager: PersistenceManager,
                     private val assetsHolder: AssetsHolder,
                     private val balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>,
                     private val balanceUpdateQueue: BlockingQueue<JsonSerializable>,
                     private val applicationSettingsCache: ApplicationSettingsCache) {

    companion object {
        private val LOGGER = Logger.getLogger(BalancesHolder::class.java.name)
    }

    lateinit var wallets: MutableMap<String, Wallet>
    var initialClientsCount = 0
    var initialBalancesCount = 0

    init {
        update()
    }

    private fun update() {
        wallets = balancesDbAccessorsHolder.primaryAccessor.loadWallets()
        initialClientsCount = wallets.size
        initialBalancesCount = wallets.values.sumBy { it.balances.size }
    }

    fun getBalance(clientId: String, assetId: String): BigDecimal {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                return balance.balance
            }
        }
        return BigDecimal.ZERO
    }

    fun getReservedBalance(clientId: String, assetId: String): BigDecimal {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                return balance.reserved
            }
        }

        return BigDecimal.ZERO
    }

    fun getAvailableBalance(clientId: String, assetId: String, reservedAdjustment: BigDecimal = BigDecimal.ZERO): BigDecimal {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                return (if (balance.reserved > reservedAdjustment)
                    balance.balance - balance.reserved + reservedAdjustment
                else balance.balance)
            }
        }

        return BigDecimal.ZERO
    }

    fun getAvailableReservedBalance(clientId: String, assetId: String): BigDecimal {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                // reserved can be greater than base balance due to transfer with overdraft
                return if (balance.reserved.signum() == 1 && balance.reserved <= balance.balance) balance.reserved else balance.balance
            }
        }

        return BigDecimal.ZERO
    }

    fun updateBalance(clientId: String, assetId: String, balance: BigDecimal): Boolean {
        val balancesUpdater = createUpdater()
        balancesUpdater.updateBalance(clientId, assetId, balance)
        val persisted = persistenceManager.persist(balancesUpdater.persistenceData())
        if (!persisted) {
            return false
        }
        balancesUpdater.apply()
        balanceUpdateNotificationQueue.add(BalanceUpdateNotification(clientId))
        return true
    }

    fun updateReservedBalance(clientId: String, assetId: String, balance: BigDecimal, skipForTrustedClient: Boolean = true): Boolean {
        val balancesUpdater = createUpdater()
        balancesUpdater.updateReservedBalance(clientId, assetId, balance)
        val persisted = persistenceManager.persist(balancesUpdater.persistenceData())
        if (!persisted) {
            return false
        }
        balancesUpdater.apply()
        balanceUpdateNotificationQueue.add(BalanceUpdateNotification(clientId))
        return true
    }

    fun insertOrUpdateWallets(wallets: Collection<Wallet>) {
        persistenceManager.persist(PersistenceData(wallets, wallets.flatMap { it.balances.values }))
        update()
    }

    fun sendBalanceUpdate(balanceUpdate: BalanceUpdate) {
        balanceUpdate.balances = balanceUpdate.balances.filter { it.newBalance != it.oldBalance || it.newReserved != it.oldReserved }
        if (balanceUpdate.balances.isNotEmpty()) {
            LOGGER.info(balanceUpdate.toString())
            balanceUpdateQueue.add(balanceUpdate)
        }
    }

    fun isTrustedClient(clientId: String) = applicationSettingsCache.isTrustedClient(clientId)

    fun createWalletProcessor(logger: Logger?, validate: Boolean = true): WalletOperationsProcessor {
        return WalletOperationsProcessor(this,
                applicationSettingsCache,
                persistenceManager,
                balanceUpdateNotificationQueue,
                assetsHolder,
                validate,
                logger)
    }

    fun createUpdater() = BalancesUpdater(this)

    fun setWallets(wallets: Collection<Wallet>) {
        wallets.forEach { wallet ->
            this.wallets[wallet.clientId] = wallet
        }
    }
}