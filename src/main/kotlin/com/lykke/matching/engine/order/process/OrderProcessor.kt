package com.lykke.matching.engine.order.process

import com.lykke.matching.engine.daos.Order
import com.lykke.matching.engine.order.transaction.ExecutionContext

interface OrderProcessor<T : Order> {
    fun processOrder(order: T, executionContext: ExecutionContext): ProcessedOrder
}