package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import java.util.HashMap

interface WalletDatabaseAccessor {
    fun loadWallets(): HashMap<String, MutableMap<String, Wallet>>
    fun insertOrUpdateWallet(wallet: Wallet)
    fun deleteWallet(wallet: Wallet)

    fun addOperation(operation: WalletOperation)
}