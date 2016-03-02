package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import java.util.HashMap

interface WalletDatabaseAccessor {
    fun loadWallets(): HashMap<String, MutableMap<String, Wallet>>
    fun insertOrUpdateWallet(wallet: Wallet) { insertOrUpdateWallets(listOf(wallet)) }
    fun insertOrUpdateWallets(wallets: List<Wallet>)
    fun deleteWallet(wallet: Wallet)

    fun insertOperation(operation: WalletOperation) { insertOperations(listOf(operation)) }
    fun insertOperations(operations: List<WalletOperation>)

    fun loadAssetPairs(): HashMap<String, AssetPair>
    fun loadAssetPair(assetId: String): AssetPair?
}