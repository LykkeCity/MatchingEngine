package com.lykke.matching.engine.balance

import com.lykke.matching.engine.daos.wallet.Wallet

interface WalletsManager {
    fun getWallet(clientId: String): Wallet?
    fun setWallets(wallets: Collection<Wallet>)
}