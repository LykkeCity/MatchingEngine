package com.lykke.matching.engine.holders

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
import java.util.LinkedList
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

    fun processWalletOperations(id: String, type: String, operations: List<WalletOperation>) {
        if (operations.isEmpty()) {
            return
        }
        val now = Date()
        val updates = HashMap<String, ClientBalanceUpdate>()
        val walletsToAdd = LinkedList<Wallet>()
        val clients = HashSet<String>()
        operations.forEach { operation ->
            val client = balances.getOrPut(operation.clientId) { HashMap() }
            val balance = client[operation.assetId]?.balance ?: 0.0
            val reservedBalance = client[operation.assetId]?.reserved ?: 0.0
            val asset = assetsHolder.getAsset(operation.assetId)

            val newBalance = RoundingUtils.parseDouble(balance + operation.amount, asset.accuracy).toDouble()
            val newReservedBalance = if (!trustedClients.contains(operation.clientId)) RoundingUtils.parseDouble(reservedBalance + operation.reservedAmount, asset.accuracy).toDouble() else reservedBalance

            client.put(operation.assetId, AssetBalance(operation.assetId, now, newBalance, newReservedBalance))

            val wallet = wallets.getOrPut(operation.clientId) { Wallet(operation.clientId) }
            wallet.setBalance(operation.assetId, now, newBalance)
            wallet.setReservedBalance(operation.assetId, now, newReservedBalance)

            if (!walletsToAdd.contains(wallet)) {
                walletsToAdd.add(wallet)
            }
            clients.add(operation.clientId)

            val update = updates.getOrPut("${operation.clientId}_${operation.assetId}") {ClientBalanceUpdate(operation.clientId, operation.assetId, balance, newBalance, reservedBalance, newReservedBalance)}
            update.newBalance = newBalance
            update.newReserved = newReservedBalance
        }

        walletDatabaseAccessor.insertOrUpdateWallets(walletsToAdd)

        clients.forEach { notificationQueue.put(BalanceUpdateNotification(it)) }

        sendBalanceUpdate(BalanceUpdate(id, type, now, updates.values.toList()))
    }

    fun updateBalance(clientId: String, assetId: String, timestamp: Date, balance: Double) {
        val client = balances.getOrPut(clientId) { HashMap() }
        val oldBalance = client[assetId]
        if (oldBalance == null) {
            client.put(assetId, AssetBalance(assetId, timestamp, balance))
        } else {
            oldBalance.balance = balance
        }

        val wallet = wallets.getOrPut(clientId) { Wallet(clientId) }
        wallet.setBalance(assetId, timestamp, balance)

        walletDatabaseAccessor.insertOrUpdateWallet(wallet)

        notificationQueue.put(BalanceUpdateNotification(clientId))
    }

    fun updateReservedBalance(clientId: String, assetId: String, timestamp: Date, balance: Double) {
        val client = balances.getOrPut(clientId) { HashMap() }
        val oldBalance = client[assetId]
        if (oldBalance == null) {
            client.put(assetId, AssetBalance(assetId, timestamp, balance, balance))
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