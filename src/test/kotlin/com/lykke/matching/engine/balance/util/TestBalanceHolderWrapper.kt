package com.lykke.matching.engine.balance.util

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.BalancesService
import org.springframework.beans.factory.annotation.Autowired
import java.math.BigDecimal

class TestBalanceHolderWrapper @Autowired constructor(private val balancesService: BalancesService,
                                                      private val balancesHolder: BalancesHolder) {


    fun updateBalance(clientId: String, assetId: String, balance: Double) {
        val wallet = balancesHolder.wallets[clientId] ?: Wallet(clientId)
        wallet.setBalance(assetId, BigDecimal.valueOf(balance) )

        balancesService.insertOrUpdateWallets(listOf(wallet), null)
    }

    fun updateReservedBalance(clientId: String, assetId: String, reservedBalance: Double) {
        val wallet = balancesHolder.wallets[clientId] ?: Wallet(clientId)
        wallet.setReservedBalance(assetId, BigDecimal.valueOf(reservedBalance) )

        balancesService.insertOrUpdateWallets(listOf(wallet), null)

    }
}