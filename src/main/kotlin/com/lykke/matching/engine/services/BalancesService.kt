package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate

interface BalancesService {
    fun insertOrUpdateWallets(wallets: Collection<Wallet>, messageSequenceNumber: Long?): Boolean
    fun sendBalanceUpdate(balanceUpdate: BalanceUpdate)
}