package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder
import java.lang.Exception

class TestOrderBookDatabaseAccessor(private val secondaryDbAccessor: TestFileOrderDatabaseAccessor): OrderBookDatabaseAccessor {

    private val orders = HashMap<String, LimitOrder>()

    override fun loadLimitOrders(): List<LimitOrder> {
        return orders.values.map { it.copy() }
    }

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        // to do nothing
    }

    fun updateOrders(ordersToSave: Collection<LimitOrder>, ordersToRemove: Collection<LimitOrder>) {
        checkDuplicates(ordersToRemove)
        checkDuplicates(ordersToSave)
        ordersToRemove.forEach { orders.remove(it.id) }
        ordersToSave.forEach { orders[it.id] = it.copy() }
    }

    private fun checkDuplicates(orders: Collection<LimitOrder>) {
        val orderIds = orders.map { it.externalId }
        if (orderIds.size != orderIds.toSet().size) {
            throw Exception("One of the orders is passed twice")
        }
    }

    fun getOrders(assetPairId: String, isBuySide: Boolean): List<LimitOrder> {
        return orders.values.filter { it.assetPairId == assetPairId && it.isBuySide() == isBuySide }
    }

    fun addLimitOrder(order: LimitOrder) {
        orders[order.id] = order
        secondaryDbAccessor.addLimitOrder(order)
    }

}