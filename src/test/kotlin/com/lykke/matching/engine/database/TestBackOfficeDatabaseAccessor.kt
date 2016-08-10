package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletCredentials
import com.lykke.matching.engine.daos.bitcoin.BtTransaction
import java.util.ArrayList
import java.util.HashMap

class TestBackOfficeDatabaseAccessor: BackOfficeDatabaseAccessor {

    val credentials = HashMap<String, WalletCredentials>()
    val assets = HashMap<String, Asset>()
    val transactions = ArrayList<BtTransaction>()

    fun addWalletCredentials(wallet: WalletCredentials) {
        credentials[wallet.clientId] = wallet
    }

    override fun loadAllWalletCredentials(): MutableMap<String, WalletCredentials> {
        val result = HashMap<String, WalletCredentials>()
        credentials.keys.forEach {
            result[it] = credentials[it]!!
        }
        return result
    }

    override fun loadWalletCredentials(clientId: String): WalletCredentials? {
        return credentials[clientId]
    }

    fun addAsset(asset: Asset) {
        assets[asset.assetId] = asset
    }

    override fun loadAsset(assetId: String): Asset? {
        return assets[assetId]
    }

    override fun loadAssets(): MutableMap<String, Asset> {
        return HashMap<String, Asset>()
    }

    override fun saveBitcoinTransaction(transaction: BtTransaction) {
        transactions.add(transaction)
    }

    fun clear() {
        credentials.clear()
        assets.clear()
        transactions.clear()
    }
}