package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.holders.BalancesHolder
import org.springframework.stereotype.Component

@Component
class CurrentTransactionBalancesHolderFactory(val balancesHolder: BalancesHolder) {
    fun create(): CurrentTransactionBalancesHolder {
        return CurrentTransactionBalancesHolder(balancesHolder)
    }
}