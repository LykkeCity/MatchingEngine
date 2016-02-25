package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder

interface LimitOrderDatabaseAccessor {
    fun loadLimitOrders(): List<LimitOrder>
    fun addLimitOrder(order: LimitOrder)
}