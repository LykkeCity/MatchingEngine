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


    override fun loadWalletCredentials(clientId: String): WalletCredentials? {
        return credentials.get(clientId)
    }

    fun addAsset(asset: Asset) {
        assets[asset.assetId] = asset
    }

    override fun loadAsset(assetId: String): Asset? {
        return assets.get(assetId)
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