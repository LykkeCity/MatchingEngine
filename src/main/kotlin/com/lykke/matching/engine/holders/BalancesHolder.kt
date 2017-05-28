package com.lykke.matching.engine.holders

import com.lykke.matching.engine.daos.WalletOperation
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

class BalancesHolder(private val walletDatabaseAccessor: WalletDatabaseAccessor,
                     private val assetsHolder: AssetsHolder,
                     private val notificationQueue: BlockingQueue<BalanceUpdateNotification>,
                     private val balanceUpdateQueue: BlockingQueue<JsonSerializable>
                     ) {

    companion object {
        val LOGGER = Logger.getLogger(BalancesHolder::class.java.name)
    }

    private val balances = walletDatabaseAccessor.loadBalances()
    private val wallets = walletDatabaseAccessor.loadWallets()

    fun getBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val balance = client[assetId]
            if (balance != null) {
                return balance
            }
        }

        return 0.0
    }

    fun processWalletOperations(id: String, type: String, operations: List<WalletOperation>) {
        val updates = HashMap<String, ClientBalanceUpdate>()
        val walletsToAdd = LinkedList<Wallet>()
        val clients = HashSet<String>()
        operations.forEach { operation ->
            val client = balances.getOrPut(operation.clientId) { HashMap<String, Double>() }
            val balance = client[operation.assetId] ?: 0.0
            val asset = assetsHolder.getAsset(operation.assetId)

            val newBalance = RoundingUtils.parseDouble(balance + operation.amount, asset.accuracy).toDouble()

            client.put(operation.assetId, newBalance)

            val wallet = wallets.getOrPut(operation.clientId) { Wallet(operation.clientId) }
            wallet.setBalance(operation.assetId, newBalance)

            if (!walletsToAdd.contains(wallet)) {
                walletsToAdd.add(wallet)
            }
            clients.add(operation.clientId)

            updates.getOrPut("${operation.clientId}_${operation.assetId}") {ClientBalanceUpdate(operation.clientId, operation.assetId, balance, newBalance)}.newBalance = newBalance
        }

        walletDatabaseAccessor.insertOrUpdateWallets(walletsToAdd)

        clients.forEach { notificationQueue.put(BalanceUpdateNotification(it)) }

        sendBalanceUpdate(BalanceUpdate(id, type, Date(), updates.values.toList()))
    }

    fun updateBalance(id: String, type: String, clientId: String, assetId: String, balance: Double) {
        val client = balances.getOrPut(clientId) { HashMap<String, Double>() }
        client.put(assetId, balance)

        val wallet = wallets.getOrPut(clientId) { Wallet(clientId) }
        val oldBalance = wallet.balances[assetId]?.balance ?: 0.0
        wallet.setBalance(assetId, balance)

        walletDatabaseAccessor.insertOrUpdateWallet(wallet)

        notificationQueue.put(BalanceUpdateNotification(clientId))

        sendBalanceUpdate(BalanceUpdate(id, type, Date(), listOf(ClientBalanceUpdate(clientId, assetId, oldBalance, balance))))
    }

    private fun sendBalanceUpdate(balanceUpdate: BalanceUpdate) {
        LOGGER.info(balanceUpdate.toString())
        balanceUpdateQueue.put(balanceUpdate)
    }
}