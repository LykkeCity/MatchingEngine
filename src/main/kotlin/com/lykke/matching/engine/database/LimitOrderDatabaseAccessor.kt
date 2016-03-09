package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder

interface LimitOrderDatabaseAccessor {
    fun loadLimitOrders(): List<LimitOrder>
    fun addLimitOrder(order: LimitOrder)
    fun updateLimitOrder(order: LimitOrder)
    fun deleteLimitOrders(orders: List<LimitOrder>)

    fun addLimitOrderDone(order: LimitOrder)
    fun addLimitOrderDoneWithGeneratedRowId(order: LimitOrder)
}