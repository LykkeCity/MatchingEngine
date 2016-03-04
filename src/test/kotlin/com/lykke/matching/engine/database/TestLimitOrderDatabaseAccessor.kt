package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder
import java.util.ArrayList

class TestLimitOrderDatabaseAccessor : LimitOrderDatabaseAccessor {

    val orders = ArrayList<LimitOrder>()
    val ordersDone = ArrayList<LimitOrder>()

    override fun loadLimitOrders(): List<LimitOrder> {
        return orders
    }

    override fun addLimitOrder(order: LimitOrder) {
        orders.add(order)
    }

    override fun updateLimitOrder(order: LimitOrder) {
        //nothing to do, already in memory
    }

    override fun deleteLimitOrders(orders: List<LimitOrder>) {
        this.orders.removeAll(orders)
    }

    override fun addLimitOrdersDone(orders: List<LimitOrder>) {
        ordersDone.addAll(orders)
    }

    fun getLastOrder() = orders.last()

    fun clear() = {
        orders.clear()
        ordersDone.clear()
    }
}