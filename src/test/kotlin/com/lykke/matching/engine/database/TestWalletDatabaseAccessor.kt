package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import java.util.HashMap
import java.util.LinkedList

class TestWalletDatabaseAccessor : WalletDatabaseAccessor {
    val wallets = HashMap<String, MutableMap<String, Wallet>>()
    val operations = LinkedList<WalletOperation>()
    val assetPairs = HashMap<String, AssetPair>()

    override fun loadWallets(): HashMap<String, MutableMap<String, Wallet>> {
        return wallets
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        wallets.forEach { wallet ->
            val client = this.wallets.getOrPut(wallet.partitionKey) { HashMap<String, Wallet>() }
            val savedWallet = client.getOrPut(wallet.rowKey) { Wallet(wallet.partitionKey, wallet.rowKey)}
            savedWallet.balance = wallet.balance
        }
    }

    override fun deleteWallet(wallet: Wallet) {
        val client = wallets[wallet.partitionKey]
        if (client != null) {
            client.remove(wallet.rowKey)
            if (client.isEmpty()) {
                wallets.remove(wallet.partitionKey)
            }
        }
    }

    fun getBalance(clientId: String, assetId: String): Double? {
        val client = wallets[clientId]
        if (client != null) {
            val wallet = client[assetId]
            if (wallet != null) {
                return wallet.balance
            }
        }
        return null
    }

    override fun insertOperations(operations: List<WalletOperation>) {
        operations.forEach { operation ->
            this.operations.add(operation)
        }
    }

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        return assetPairs
    }

    override fun loadAssetPair(assetId: String): AssetPair {
        return assetPairs[assetId]!!
    }

    fun addAssetPair(pair: AssetPair) {
        assetPairs[pair.getAssetPairId()] = pair
    }

    fun clear() {
        wallets.clear()
        operations.clear()
        assetPairs.clear()
    }
}