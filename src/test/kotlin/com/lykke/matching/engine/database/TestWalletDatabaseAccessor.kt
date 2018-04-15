package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import java.util.HashMap

class TestWalletDatabaseAccessor : WalletDatabaseAccessor {

    private val balances = HashMap<String, MutableMap<String, AssetBalance>>()
    private val wallets = HashMap<String, Wallet>()

    override fun loadBalances(): HashMap<String, MutableMap<String, AssetBalance>> {
        return balances.mapValues { clientBalanceEntry ->
            clientBalanceEntry.value.mapValues { assetBalanceEntry ->
                val assetBalance = assetBalanceEntry.value
                AssetBalance(assetBalance.asset, assetBalance.balance, assetBalance.reserved)
            }.toMutableMap()
        }.toMap(HashMap())
    }

    override fun loadWallets(): HashMap<String, Wallet> {
        return wallets.mapValues { walletEntry ->
            val wallet = walletEntry.value
            Wallet(wallet.clientId, wallet.balances.map { assetBalanceEntry ->
                val assetId = assetBalanceEntry.key
                val assetBalance = assetBalanceEntry.value
                AssetBalance(assetId, assetBalance.balance, assetBalance.reserved)
            })
        }.toMap(HashMap())
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        val clientIds = wallets.map { it.clientId }
        if (clientIds.size != clientIds.toSet().size) {
            throw Exception("Wallets list contains several wallets with the same client")
        }

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

    fun clear() {
        balances.clear()
        wallets.clear()
    }

}
fun buildWallet(clientId: String, assetId: String, balance: Double, reservedBalance: Double = 0.0): Wallet {
    val wallet = Wallet(clientId)
    wallet.setBalance(assetId, balance)
    wallet.setReservedBalance(assetId, reservedBalance)
    return wallet
}