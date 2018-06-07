package com.lykke.matching.engine.updaters

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.common.entity.BalancesData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.holders.BalancesHolder
import java.math.BigDecimal

class BalancesUpdater(private val balancesHolder: BalancesHolder) {

    private val changedBalances = mutableMapOf<String, MutableMap<String, AssetBalance>>()
    private val changedWallets = mutableMapOf<String, Wallet>()

    fun updateBalance(clientId: String, assetId: String, balance: BigDecimal) {
        val walletAssetBalance = getWalletAssetBalance(clientId, assetId)
        walletAssetBalance.assetBalance.balance = balance
    }

    fun updateReservedBalance(clientId: String, assetId: String, balance: BigDecimal) {
        val walletAssetBalance = getWalletAssetBalance(clientId, assetId)
        walletAssetBalance.assetBalance.reserved = balance
    }

    fun persistenceData(): BalancesData {
        return BalancesData(changedWallets.values, changedBalances.flatMap { it.value.values })
    }

    fun apply() {
        balancesHolder.setWallets(changedWallets.values)
    }

    fun getWalletAssetBalance(clientId: String, assetId: String): WalletAssetBalance {
        val wallet = changedWallets.getOrPut(clientId) {
            copyWallet(balancesHolder.wallets[clientId]) ?: Wallet(clientId)
        }
        val assetBalance = changedBalances
                .getOrPut(clientId) {
                    mutableMapOf()
                }
                .getOrPut(assetId) {
                    wallet.balances.getOrPut(assetId) { AssetBalance(clientId, assetId) }
                }
        return WalletAssetBalance(wallet, assetBalance)
    }

    private fun copyWallet(wallet: Wallet?): Wallet? {
        if (wallet == null) {
            return null
        }
        return Wallet(wallet.clientId, wallet.balances.values.map { copyAssetBalance(it) })
    }

    private fun copyAssetBalance(assetBalance: AssetBalance): AssetBalance {
        return AssetBalance(assetBalance.clientId,
                assetBalance.asset,
                assetBalance.balance,
                assetBalance.reserved)
    }
}

class WalletAssetBalance(val wallet: Wallet, val assetBalance: AssetBalance)