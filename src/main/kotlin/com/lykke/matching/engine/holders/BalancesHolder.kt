package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.utils.RoundingUtils
import org.apache.log4j.Logger
import java.util.Date
import java.util.HashMap
import java.util.HashSet
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap

class BalancesHolder(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                     private val assetsHolder: AssetsHolder,
                     private val notificationQueue: BlockingQueue<BalanceUpdateNotification>,
                     private val balanceUpdateQueue: BlockingQueue<JsonSerializable>,
                     private val trustedClients: Set<String>) {

    companion object {
        private val LOGGER = Logger.getLogger(BalancesHolder::class.java.name)
    }

    private val balances = walletDatabaseAccessor.loadBalances()
    val wallets = walletDatabaseAccessor.loadWallets().toMap(ConcurrentHashMap())

    val initialClientsCount: Int = balances.size
    val initialBalancesCount: Int = balances.values.sumBy { it.size }

    fun getBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val balance = client[assetId]
            if (balance != null) {
                return balance.balance
            }
        }

        return 0.0
    }

    fun getReservedBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val balance = client[assetId]
            if (balance != null) {
                return balance.reserved
            }
        }

        return 0.0
    }

    fun getAvailableBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val balance = client[assetId]
            if (balance != null) {
                return if (balance.reserved > 0.0) balance.balance - balance.reserved else balance.balance
            }
        }

        return 0.0
    }

    fun getAvailableReservedBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val balance = client[assetId]
            if (balance != null) {
                return if (balance.reserved > 0.0) balance.reserved else balance.balance
            }
        }

        return 0.0
    }

    fun preProcessWalletOperations(operations: List<WalletOperation>, previousResult: PreProcessWalletOperationsResult? = null, validate: Boolean = true): PreProcessWalletOperationsResult {
        if (operations.isEmpty()) {
            return previousResult ?: PreProcessWalletOperationsResult(validate)
        }
        val now = previousResult?.timestamp ?: Date()
        val updates = previousResult?.updates?.toMutableMap() ?: HashMap()
        val clients = previousResult?.clients?.toMutableSet() ?: HashSet()
        val balances = previousResult?.balances?.toMutableMap() ?: HashMap()
        val wallets = previousResult?.wallets?.toMutableMap() ?: HashMap()
        operations.forEach { operation ->
            val client = balances.getOrPut(operation.clientId) {
                this.balances.getOrPut(operation.clientId) { HashMap() }
                        .mapValues { CopyWrapper(it.value) }
                        .toMutableMap()
            }
            val assetBalanceWrapper = client[operation.assetId] ?: CopyWrapper(AssetBalance(operation.assetId, now, 0.0, 0.0), true)
            val assetBalanceCopy = assetBalanceWrapper.copy

            val balance = assetBalanceCopy.balance
            val reservedBalance = assetBalanceCopy.reserved
            val asset = assetsHolder.getAsset(operation.assetId)

            assetBalanceCopy.balance = RoundingUtils.parseDouble(balance + operation.amount, asset.accuracy).toDouble()
            assetBalanceCopy.reserved = if (!trustedClients.contains(operation.clientId)) RoundingUtils.parseDouble(reservedBalance + operation.reservedAmount, asset.accuracy).toDouble() else reservedBalance

            client[operation.assetId] = assetBalanceWrapper

            val walletWrapper = wallets.getOrPut(operation.clientId) {
                CopyWrapper(this.wallets.getOrPut(operation.clientId) {
                    Wallet(operation.clientId)
                })
            }
            val walletCopy = walletWrapper.copy
            walletCopy.setBalance(operation.assetId, now, assetBalanceCopy.balance)
            walletCopy.setReservedBalance(operation.assetId, now, assetBalanceCopy.reserved)

            clients.add(operation.clientId)

            val update = updates.getOrPut("${operation.clientId}_${operation.assetId}") { ClientBalanceUpdate(operation.clientId, operation.assetId, balance, assetBalanceCopy.balance, reservedBalance, assetBalanceCopy.reserved) }
            update.newBalance = assetBalanceCopy.balance
            update.newReserved = assetBalanceCopy.reserved
        }

        return PreProcessWalletOperationsResult(balances, wallets, clients, updates, now, validate)
    }

    fun confirmWalletOperations(id: String, type: String, preProcessResult: PreProcessWalletOperationsResult) {
        if (preProcessResult.empty) {
            return
        }

        val updatedWallets = preProcessResult.wallets!!.mapValues { it.value.applyToOrigin() }
        wallets.putAll(updatedWallets)
        balances.putAll(preProcessResult.balances!!.mapValues { it.value.mapValues { it.value.applyToOrigin() }.toMutableMap() })
        walletDatabaseAccessor.insertOrUpdateWallets(updatedWallets.values.toList())
        preProcessResult.clients!!.forEach { notificationQueue.put(BalanceUpdateNotification(it)) }
        sendBalanceUpdate(BalanceUpdate(id, type, preProcessResult.timestamp!!, preProcessResult.updates!!.values.toList()))
    }

    fun updateBalance(clientId: String, assetId: String, timestamp: Date, balance: Double) {
        val client = balances.getOrPut(clientId) { HashMap() }
        val oldBalance = client[assetId]
        if (oldBalance == null) {
            client[assetId] = AssetBalance(assetId, timestamp, balance)
        } else {
            oldBalance.balance = balance
        }

        val wallet = wallets.getOrPut(clientId) { Wallet(clientId) }
        wallet.setBalance(assetId, timestamp, balance)

        walletDatabaseAccessor.insertOrUpdateWallet(wallet)

        notificationQueue.put(BalanceUpdateNotification(clientId))
    }

    fun updateReservedBalance(clientId: String, assetId: String, timestamp: Date, balance: Double, skipForTrustedClient: Boolean = true) {
        if (skipForTrustedClient && trustedClients.contains(clientId)) {
            return
        }
        val client = balances.getOrPut(clientId) { HashMap() }
        val oldBalance = client[assetId]
        if (oldBalance == null) {
            client[assetId] = AssetBalance(assetId, timestamp, balance, balance)
        } else {
            oldBalance.reserved = balance
        }

        val wallet = wallets.getOrPut(clientId) { Wallet(clientId) }
        wallet.setReservedBalance(assetId, timestamp, balance)

        walletDatabaseAccessor.insertOrUpdateWallet(wallet)

        notificationQueue.put(BalanceUpdateNotification(clientId))
    }

    fun sendBalanceUpdate(balanceUpdate: BalanceUpdate) {
        LOGGER.info(balanceUpdate.toString())
        balanceUpdateQueue.put(balanceUpdate)
    }
}