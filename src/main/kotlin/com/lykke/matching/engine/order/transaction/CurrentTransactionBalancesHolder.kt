package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.common.entity.BalancesData
import com.lykke.matching.engine.holders.BalancesHolder
import java.math.BigDecimal

class CurrentTransactionBalancesHolder(private val balancesHolder: BalancesHolder) {

    private val changedBalancesByClientIdAndAssetId = mutableMapOf<String, MutableMap<String, AssetBalance>>()
    private val changedWalletsByClientId = mutableMapOf<String, Wallet>()

    fun updateBalance(clientId: String, assetId: String, balance: BigDecimal) {
        val walletAssetBalance = getWalletAssetBalance(clientId, assetId)
        walletAssetBalance.assetBalance.balance = balance
    }

    fun updateReservedBalance(clientId: String, assetId: String, balance: BigDecimal) {
        val walletAssetBalance = getWalletAssetBalance(clientId, assetId)
        walletAssetBalance.assetBalance.reserved = balance
    }

    fun persistenceData(): BalancesData {
        return BalancesData(changedWalletsByClientId.values, changedBalancesByClientIdAndAssetId.flatMap { it.value.values })
    }

    fun apply() {
        balancesHolder.setWallets(changedWalletsByClientId.values)
    }

    fun getWalletAssetBalance(clientId: String, assetId: String): WalletAssetBalance {
        val wallet = changedWalletsByClientId.getOrPut(clientId) {
            copyWallet(balancesHolder.wallets[clientId]) ?: Wallet(clientId)
        }
        val assetBalance = changedBalancesByClientIdAndAssetId
                .getOrPut(clientId) {
                    mutableMapOf()
                }
                .getOrPut(assetId) {
                    wallet.balances.getOrPut(assetId) { AssetBalance(clientId, assetId) }
                }
        return WalletAssetBalance(wallet, assetBalance)
    }

    fun getChangedCopyOrOriginalAssetBalance(clientId: String, assetId: String): AssetBalance {
        return (changedWalletsByClientId[clientId] ?: balancesHolder.wallets[clientId] ?: Wallet(clientId)).balances[assetId]
                ?: AssetBalance(clientId, assetId)
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