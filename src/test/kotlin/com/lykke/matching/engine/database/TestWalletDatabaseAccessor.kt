package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.HashMap

@Component
class TestWalletDatabaseAccessor : WalletDatabaseAccessor {

    private val wallets = HashMap<String, Wallet>()

    override fun loadWallets(): HashMap<String, Wallet> {
        return wallets.mapValues { walletEntry ->
            val wallet = walletEntry.value
            Wallet(wallet.clientId, wallet.balances.map { assetBalanceEntry ->
                val assetId = assetBalanceEntry.key
                val assetBalance = assetBalanceEntry.value
                AssetBalance(wallet.clientId, assetId, assetBalance.balance, assetBalance.reserved)
            })
        }.toMap(HashMap())
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        val clientIds = wallets.map { it.clientId }
        if (clientIds.size != clientIds.toSet().size) {
            throw Exception("Wallets list contains several wallets with the same client")
        }

        wallets.forEach { wallet ->
            val updatedWallet = this.wallets.getOrPut(wallet.clientId) { Wallet(wallet.clientId) }
            wallet.balances.values.forEach {
                updatedWallet.setBalance(it.asset, it.balance)
                updatedWallet.setReservedBalance(it.asset, it.reserved)
            }
        }
    }

    fun getBalance(clientId: String, assetId: String): BigDecimal {
        val client = wallets[clientId]?.balances
        if (client != null) {
            val wallet = client[assetId]
            if (wallet != null) {
                return wallet.balance
            }
        }
        return BigDecimal.ZERO
    }


    fun getReservedBalance(clientId: String, assetId: String): BigDecimal {
        val client = wallets[clientId]?.balances
        if (client != null) {
            val wallet = client[assetId]
            if (wallet != null) {
                return wallet.reserved
            }
        }
        return BigDecimal.ZERO
    }

    fun clear() {
        wallets.clear()
    }

}
fun buildWallet(clientId: String, assetId: String, balance: Double, reservedBalance: Double = 0.0): Wallet {
    val wallet = Wallet(clientId)
    wallet.setBalance(assetId, balance.toBigDecimal())
    wallet.setReservedBalance(assetId, reservedBalance.toBigDecimal())
    return wallet
}