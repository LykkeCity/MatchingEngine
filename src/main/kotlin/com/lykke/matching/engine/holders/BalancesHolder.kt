package com.lykke.matching.engine.holders

import com.lykke.matching.engine.balance.WalletOperationsProcessor
import com.lykke.matching.engine.daos.wallet.ClientAssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.common.PersistenceData
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import org.apache.log4j.Logger
import java.util.concurrent.BlockingQueue

class BalancesHolder(walletDatabaseAccessor: WalletDatabaseAccessor,
                     private val persistenceManager: PersistenceManager,
                     private val assetsHolder: AssetsHolder,
                     private val notificationQueue: BlockingQueue<BalanceUpdateNotification>,
                     private val balanceUpdateQueue: BlockingQueue<JsonSerializable>,
                     private val applicationSettingsCache: ApplicationSettingsCache) {

    companion object {
        private val LOGGER = Logger.getLogger(BalancesHolder::class.java.name)
    }

    val wallets = walletDatabaseAccessor.loadWallets()
    val initialClientsCount: Int = wallets.size
    val initialBalancesCount: Int = wallets.values.sumBy { it.balances.size }

    fun getBalance(clientId: String, assetId: String): Double {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                return balance.balance
            }
        }
        return 0.0
    }

    fun getReservedBalance(clientId: String, assetId: String): Double {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                return balance.reserved
            }
        }

        return 0.0
    }

    fun getAvailableBalance(clientId: String, assetId: String): Double {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                return if (balance.reserved > 0.0) balance.balance - balance.reserved else balance.balance
            }
        }

        return 0.0
    }

    fun getAvailableReservedBalance(clientId: String, assetId: String): Double {
        val wallet = wallets[clientId]
        if (wallet != null) {
            val balance = wallet.balances[assetId]
            if (balance != null) {
                // reserved can be greater than base balance due to transfer with overdraft
                return if (balance.reserved > 0.0 && balance.reserved <= balance.balance) balance.reserved else balance.balance
            }
        }

        return 0.0
    }

    fun updateBalance(clientId: String, assetId: String, balance: Double) {
        val wallet = wallets.getOrPut(clientId) { Wallet(clientId) }
        wallet.setBalance(assetId, balance)
        persistenceManager.persist(PersistenceData(
                listOf(wallet),
                listOf(ClientAssetBalance(clientId, wallet.balances[assetId]!!))
        ))
        notificationQueue.put(BalanceUpdateNotification(clientId))
    }

    fun updateReservedBalance(clientId: String, assetId: String, balance: Double, skipForTrustedClient: Boolean = true) {
        if (skipForTrustedClient && applicationSettingsCache.isTrustedClient(clientId)) {
            return
        }

        val wallet = wallets.getOrPut(clientId) { Wallet(clientId) }
        wallet.setReservedBalance(assetId, balance)
        persistenceManager.persist(PersistenceData(
                listOf(wallet),
                listOf(ClientAssetBalance(clientId, wallet.balances[assetId]!!))
        ))
        notificationQueue.put(BalanceUpdateNotification(clientId))
    }

    fun sendBalanceUpdate(balanceUpdate: BalanceUpdate) {
        LOGGER.info(balanceUpdate.toString())
        balanceUpdateQueue.put(balanceUpdate)
    }

    fun isTrustedClient(clientId: String) = applicationSettingsCache.isTrustedClient(clientId)

    fun createWalletProcessor(logger: Logger?, validate: Boolean = true): WalletOperationsProcessor {
        return WalletOperationsProcessor(this, persistenceManager, notificationQueue, assetsHolder, validate, logger)
    }
}