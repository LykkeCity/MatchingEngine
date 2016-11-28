package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.wallet.Wallet
import java.util.HashMap
import java.util.LinkedList

class TestWalletDatabaseAccessor : WalletDatabaseAccessor {

    val balances = HashMap<String, MutableMap<String, Double>>()
    val wallets = HashMap<String, Wallet>()
    val operations = LinkedList<WalletOperation>()
    val externalOperations = LinkedList<ExternalCashOperation>()
    val assetPairs = HashMap<String, AssetPair>()

    override fun loadBalances(): HashMap<String, MutableMap<String, Double>> {
        return balances
    }

    override fun loadWallets(): HashMap<String, Wallet> {
        return wallets
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        wallets.forEach { wallet ->
            val client = balances.getOrPut(wallet.clientId,  { HashMap<String, Double>() })
            val updatedWallet = this.wallets.getOrPut(wallet.clientId) { Wallet(wallet.clientId) }
            wallet.balances.values.forEach {
                client.put(it.asset, it.balance)
                updatedWallet.setBalance(it.asset, it.balance)
            }
        }
    }

    fun getBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val wallet = client[assetId]
            if (wallet != null) {
                return wallet
            }
        }
        return 0.0
    }

    override fun loadExternalCashOperation(clientId: String, operationId: String): ExternalCashOperation? {
        return externalOperations.find { it.clientId == clientId && it.externalId == operationId }
    }

    override fun insertExternalCashOperation(operation: ExternalCashOperation) {
        this.externalOperations.add(operation)
    }

    override fun insertOperation(operation: WalletOperation) {
        this.operations.add(operation)
    }

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        return assetPairs
    }

    override fun loadAssetPair(assetId: String): AssetPair {
        return assetPairs[assetId]!!
    }

    fun addAssetPair(pair: AssetPair) {
        assetPairs[pair.assetPairId] = pair
    }

    fun clear() {
        balances.clear()
        wallets.clear()
        operations.clear()
        externalOperations.clear()
        assetPairs.clear()
    }

}
fun buildWallet(clientId: String, assetId: String, balance: Double): Wallet {
    val wallet = Wallet(clientId)
    wallet.setBalance(assetId, balance)
    return wallet
}