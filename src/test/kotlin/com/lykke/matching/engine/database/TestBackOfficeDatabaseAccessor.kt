package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.BtTransaction
import com.lykke.matching.engine.daos.WalletCredentials
import java.util.*

class TestBackOfficeDatabaseAccessor: BackOfficeDatabaseAccessor {

    val credentials = HashMap<String, WalletCredentials>()
    val assets = HashMap<String, Asset>()
    val transactions = ArrayList<BtTransaction>()


    override fun loadWalletCredentials(clientId: String): WalletCredentials? {
        return credentials.get(clientId)
    }

    override fun loadAsset(assetId: String): Asset? {
        return assets.get(assetId)
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