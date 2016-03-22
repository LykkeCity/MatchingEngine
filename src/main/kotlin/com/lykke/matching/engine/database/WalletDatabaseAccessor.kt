package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import java.util.HashMap

interface WalletDatabaseAccessor {
    fun loadBalances(): HashMap<String, MutableMap<String, Double>>
    fun loadWallets(): HashMap<String, Wallet>
    fun insertOrUpdateWallet(wallet: Wallet) { insertOrUpdateWallets(listOf(wallet)) }
    fun insertOrUpdateWallets(wallets: List<Wallet>)

    fun insertExternalCashOperation(operation: ExternalCashOperation)
    fun loadExternalCashOperation(clientId: String, operationId: String): ExternalCashOperation?
    fun insertOperation(operation: WalletOperation)

    fun loadAssetPairs(): HashMap<String, AssetPair>
    fun loadAssetPair(assetId: String): AssetPair?
}