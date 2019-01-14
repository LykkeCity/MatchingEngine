package com.lykke.matching.engine.order.process.common

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.transaction.ExecutionContext

interface LimitOrdersCanceller {

    fun cancelOrders(limitOrdersToCancel: Collection<LimitOrder>,
                     limitOrdersToReplace: Collection<LimitOrder>,
                     stopLimitOrdersToCancel: Collection<LimitOrder>,
                     stopLimitOrdersToReplace: Collection<LimitOrder>,
                     executionContext: ExecutionContext)
}