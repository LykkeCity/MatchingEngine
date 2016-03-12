package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import java.util.*

class TestWalletDatabaseAccessor : WalletDatabaseAccessor {

    val balances = HashMap<String, MutableMap<String, Double>>()
    val wallets = HashMap<String, Wallet>()
    val operations = LinkedList<WalletOperation>()
    val assetPairs = HashMap<String, AssetPair>()

    override fun loadBalances(): HashMap<String, MutableMap<String, Double>> {
        return balances
    }

    override fun loadWallets(): HashMap<String, Wallet> {
        return wallets
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        wallets.forEach { wallet ->
            val client = balances.getOrPut(wallet.getClientId()) { HashMap<String, Double>() }
            val updatedWallet = this.wallets.getOrPut(wallet.getClientId()) { Wallet( wallet.getClientId() ) }
            wallet.getBalances().forEach {
                client.put(it.Asset, it.Balance)
                updatedWallet.setBalance(it.Asset, it.Balance)
            }
        }
    }

    fun getBalance(clientId: String, assetId: String): Double? {
        val client = balances[clientId]
        if (client != null) {
            val wallet = client[assetId]
            if (wallet != null) {
                return wallet
            }
        }
        return null
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
        assetPairs[pair.getAssetPairId()] = pair
    }

    fun clear() {
        balances.clear()
        wallets.clear()
        operations.clear()
        assetPairs.clear()
    }

}
fun buildWallet(clientId: String, assetId: String, balance: Double): Wallet {
    val wallet = Wallet(clientId)
    wallet.addBalance(assetId, balance)
    return wallet
}