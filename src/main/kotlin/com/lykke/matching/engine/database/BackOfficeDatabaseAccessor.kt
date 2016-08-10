package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletCredentials
import com.lykke.matching.engine.daos.bitcoin.BtTransaction

interface BackOfficeDatabaseAccessor {
    fun loadAllWalletCredentials(): MutableMap<String, WalletCredentials>
    fun loadWalletCredentials(clientId: String): WalletCredentials?
    fun loadAssets(): MutableMap<String, Asset>
    fun loadAsset(assetId: String): Asset?
    fun saveBitcoinTransaction(transaction: BtTransaction)
}