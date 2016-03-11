package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.BtTransaction
import com.lykke.matching.engine.daos.WalletCredentials

interface BackOfficeDatabaseAccessor {
    fun loadWalletCredentials(clientId: String): WalletCredentials?
    fun loadAsset(assetId: String): Asset?
    fun saveBitcoinTransaction(transaction: BtTransaction)
}