package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.NewLimitOrder

class TestOrderBookDatabaseAccessor(private val secondaryDbAccessor: TestFileOrderDatabaseAccessor): OrderBookDatabaseAccessor {

    private val orders = HashMap<String, NewLimitOrder>()

    override fun loadLimitOrders(): List<NewLimitOrder> {
        return orders.values.map { it.copy() }
    }

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>) {
        // to do nothing
    }

    fun updateOrders(ordersToSave: Collection<NewLimitOrder>, ordersToRemove: Collection<NewLimitOrder>) {
        ordersToRemove.forEach { orders.remove(it.id) }
        ordersToSave.forEach { orders[it.id] = it.copy() }
    }

    fun getOrders(assetPairId: String, isBuySide: Boolean): List<NewLimitOrder> {
        return orders.values.filter { it.assetPairId == assetPairId && it.isBuySide() == isBuySide }
    }

    fun addLimitOrder(order: NewLimitOrder) {
        orders[order.id] = order
        secondaryDbAccessor.addLimitOrder(order)
    }

}