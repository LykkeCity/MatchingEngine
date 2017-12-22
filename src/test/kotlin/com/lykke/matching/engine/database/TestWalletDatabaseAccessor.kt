package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import java.util.HashMap
import java.util.LinkedList

class TestWalletDatabaseAccessor : WalletDatabaseAccessor {

    private val balances = HashMap<String, MutableMap<String, AssetBalance>>()
    private val wallets = HashMap<String, Wallet>()
    private val transferOperations = LinkedList<TransferOperation>()
    private val assetPairs = HashMap<String, AssetPair>()

    override fun loadBalances(): HashMap<String, MutableMap<String, AssetBalance>> {
        return balances
    }

    override fun loadWallets(): HashMap<String, Wallet> {
        return wallets
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        wallets.forEach { wallet ->
            val client = balances.getOrPut(wallet.clientId,  { HashMap<String, AssetBalance>() })
            val updatedWallet = this.wallets.getOrPut(wallet.clientId) { Wallet(wallet.clientId) }
            wallet.balances.values.forEach {
                client.put(it.asset, AssetBalance(it.asset, it.balance, it.reserved))
                updatedWallet.setBalance(it.asset, it.balance)
                updatedWallet.setReservedBalance(it.asset, it.reserved)
            }
        }
    }

    fun getBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val wallet = client[assetId]
            if (wallet != null) {
                return wallet.balance
            }
        }
        return 0.0
    }


    fun getReservedBalance(clientId: String, assetId: String): Double {
        val client = balances[clientId]
        if (client != null) {
            val wallet = client[assetId]
            if (wallet != null) {
                return wallet.reserved
            }
        }
        return 0.0
    }

    override fun insertTransferOperation(operation: TransferOperation) {
        this.transferOperations.add(operation)
    }

    override fun insertSwapOperation(operation: SwapOperation) {
        throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
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
        transferOperations.clear()
        assetPairs.clear()
    }

}
fun buildWallet(clientId: String, assetId: String, balance: Double, reservedBalance: Double = 0.0): Wallet {
    val wallet = Wallet(clientId)
    wallet.setBalance(assetId, balance)
    wallet.setReservedBalance(assetId, reservedBalance)
    return wallet
}