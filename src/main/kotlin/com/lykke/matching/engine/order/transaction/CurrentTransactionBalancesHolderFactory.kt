package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.balance.WalletsManager
import com.lykke.matching.engine.holders.BalancesHolder
import org.springframework.stereotype.Component

@Component
class CurrentTransactionBalancesHolderFactory(val balancesHolder: BalancesHolder) {
    fun create(walletsManager: WalletsManager?): CurrentTransactionBalancesHolder {
        val resWalletsManager = walletsManager ?: balancesHolder
        return CurrentTransactionBalancesHolder(resWalletsManager)
    }
}