package com.lykke.matching.engine.balance.util

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import org.springframework.beans.factory.annotation.Autowired

class TestBalanceHolderWrapper @Autowired constructor (private val balanceUpdateHandlerTest: BalanceUpdateHandlerTest,
                                                       private val balancesHolder: BalancesHolder)  {

    fun updateBalance(clientId: String, assetId: String, balance: Double) {
        balancesHolder.updateBalance(clientId, assetId, balance)
        balanceUpdateHandlerTest.clear()
    }

    fun updateReservedBalance(clientId: String, assetId: String, reservedBalance: Double, skip: Boolean = false) {
        balancesHolder.updateReservedBalance(clientId, assetId, reservedBalance, skip)
        balanceUpdateHandlerTest.clear()
    }
}