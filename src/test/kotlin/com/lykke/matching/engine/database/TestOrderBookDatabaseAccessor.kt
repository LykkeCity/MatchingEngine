package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder

class TestOrderBookDatabaseAccessor(private val secondaryDbAccessor: TestFileOrderDatabaseAccessor): OrderBookDatabaseAccessor {

    private val orders = HashMap<String, LimitOrder>()

    override fun loadLimitOrders(): List<LimitOrder> {
        return orders.values.map { it.copy() }
    }

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        // to do nothing
    }

    fun updateOrders(ordersToSave: Collection<LimitOrder>, ordersToRemove: Collection<LimitOrder>) {
        ordersToRemove.forEach { orders.remove(it.id) }
        ordersToSave.forEach { orders[it.id] = it.copy() }
    }

    fun getOrders(assetPairId: String, isBuySide: Boolean): List<LimitOrder> {
        return orders.values.filter { it.assetPairId == assetPairId && it.isBuySide() == isBuySide }
    }

    fun addLimitOrder(order: LimitOrder) {
        orders[order.id] = order
        secondaryDbAccessor.addLimitOrder(order)
    }

}