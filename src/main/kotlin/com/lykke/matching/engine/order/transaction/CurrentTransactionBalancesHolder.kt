package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.balance.WalletsManager
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.common.entity.BalancesData
import java.math.BigDecimal

class CurrentTransactionBalancesHolder(private val walletsManager: WalletsManager): WalletsManager {

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
        val prevBalanceData =  if (walletsManager is CurrentTransactionBalancesHolder) {
            walletsManager.persistenceData()
        } else null

        return mergeBalancesData(BalancesData(changedWalletsByClientId.values, changedBalancesByClientIdAndAssetId.flatMap { it.value.values }), prevBalanceData)
    }

    fun apply() {
        walletsManager.setWallets(changedWalletsByClientId.values)
    }

    fun createCurrenTransactionBalancesHolder(): CurrentTransactionBalancesHolder {
        return CurrentTransactionBalancesHolder(this)
    }

    fun getWalletAssetBalance(clientId: String, assetId: String): WalletAssetBalance {
        val wallet = changedWalletsByClientId.getOrPut(clientId) {
            copyWallet(walletsManager.getWallet(clientId)) ?: Wallet(clientId)
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
        return getChangedCopyOrOriginalWallet(clientId).balances[assetId]
                ?: AssetBalance(clientId, assetId)
    }

    fun getChangedCopyOrOriginalWallet(clientId: String): Wallet {
        return changedWalletsByClientId[clientId] ?: walletsManager.getWallet(clientId) ?: Wallet(clientId)
    }

    override fun getWallet(clientId: String): Wallet? {
        return changedWalletsByClientId[clientId] ?: walletsManager.getWallet(clientId)
    }

    override fun setWallets(wallets: Collection<Wallet>) {
        wallets.forEach {
            changedWalletsByClientId[it.clientId] = it
        }
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

    private fun mergeBalancesData(newBalancesData: BalancesData, oldBalancesData: BalancesData?): BalancesData {
        if (oldBalancesData == null) {
            return newBalancesData
        }

        val clientIdToAssetIdFromNewData = newBalancesData.balances.map { "${it.clientId} it.asset}" }.toSet()
        val clientIdsFromNewWallets = newBalancesData.wallets.map { it.clientId }.toSet()

        val resultWalletList = ArrayList<Wallet>(newBalancesData.wallets)
        val resultAssetBalanceList = ArrayList<AssetBalance>(newBalancesData.balances)

        oldBalancesData.wallets.forEach {
            if (!clientIdsFromNewWallets.contains(it.clientId)) {
                resultWalletList.add(it)
            }
        }

        oldBalancesData.balances.forEach {
            if (!clientIdToAssetIdFromNewData.contains("${it.clientId}_${it.asset}")) {
                resultAssetBalanceList.add(it)
            }
        }

        return BalancesData(resultWalletList, resultAssetBalanceList)
    }
}

class WalletAssetBalance(val wallet: Wallet, val assetBalance: AssetBalance)