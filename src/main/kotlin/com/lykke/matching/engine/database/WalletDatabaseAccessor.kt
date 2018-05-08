package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.wallet.Wallet
import java.util.HashMap

interface WalletDatabaseAccessor {
    fun loadWallets(): HashMap<String, Wallet>
    fun insertOrUpdateWallet(wallet: Wallet) { insertOrUpdateWallets(listOf(wallet)) }
    fun insertOrUpdateWallets(wallets: List<Wallet>)
}