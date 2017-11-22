package com.lykke.matching.engine.daos.file.wallet

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import java.io.Serializable
import java.util.HashMap

class FileWallet(wallet: Wallet): Serializable {
    val clientId: String = wallet.clientId
    val balances: MutableMap<String, FileAssetBalance> = HashMap()

    init {
        wallet.balances.mapValuesTo(balances) {
            val assetBalance = it.value
            FileAssetBalance(assetBalance.asset, assetBalance.timestamp, assetBalance.balance, assetBalance.reserved)
        }
    }

    fun toWallet(): Wallet {
        val balances = this.balances.values.map { fileAssetBalance ->
            AssetBalance(fileAssetBalance.asset, fileAssetBalance.timestamp, fileAssetBalance.balance, fileAssetBalance.reserved)
        }
        return Wallet(clientId, balances)
    }
}