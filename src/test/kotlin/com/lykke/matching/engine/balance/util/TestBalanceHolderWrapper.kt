package com.lykke.matching.engine.balance.util

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import org.springframework.beans.factory.annotation.Autowired
import java.math.BigDecimal

class TestBalanceHolderWrapper @Autowired constructor (private val balanceUpdateHandlerTest: BalanceUpdateHandlerTest,
                                                       private val balancesHolder: BalancesHolder)  {

    fun updateBalance(clientId: String, assetId: String, balance: Double) {
        balancesHolder.updateBalance(clientId, assetId, BigDecimal.valueOf(balance))
        balanceUpdateHandlerTest.clear()
    }

    fun updateReservedBalance(clientId: String, assetId: String, reservedBalance: Double, skip: Boolean = false) {
        balancesHolder.updateReservedBalance(clientId, assetId, BigDecimal.valueOf(reservedBalance), skip)
        balanceUpdateHandlerTest.clear()
    }
}