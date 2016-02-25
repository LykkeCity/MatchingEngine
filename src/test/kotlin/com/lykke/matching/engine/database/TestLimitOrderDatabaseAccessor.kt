package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder
import java.util.ArrayList

class TestLimitOrderDatabaseAccessor : LimitOrderDatabaseAccessor {

    val orders = ArrayList<LimitOrder>()

    override fun loadLimitOrders(): List<LimitOrder> {
        return orders
    }

    override fun addLimitOrder(order: LimitOrder) {
        orders.add(order)
    }
}