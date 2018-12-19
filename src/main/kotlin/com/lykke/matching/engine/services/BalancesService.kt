package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.wallet.Wallet

interface BalancesService {
    fun insertOrUpdateWallets(wallets: Collection<Wallet>, messageSequenceNumber: Long?): Boolean
}