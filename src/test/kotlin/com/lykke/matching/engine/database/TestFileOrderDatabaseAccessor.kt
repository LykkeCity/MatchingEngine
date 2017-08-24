package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.NewLimitOrder
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

class TestFileOrderDatabaseAccessor: OrderBookDatabaseAccessor {

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: PriorityBlockingQueue<NewLimitOrder>) {
        orders["$asset-$isBuy"] = orderBook.toMutableList()
    }

    private val orders = HashMap<String, MutableList<NewLimitOrder>>()

    fun addLimitOrder(order: NewLimitOrder) {
        orders.getOrPut("${order.assetPairId}-${order.isBuySide()}") {ArrayList()}.add(order)
    }
    override fun loadLimitOrders(): List<NewLimitOrder> {
        val result = LinkedList<NewLimitOrder>()
        orders.values.forEach { result.addAll(it) }
        return result
    }

    fun clear() {
        orders.clear()
    }

    fun getOrders(asset: String, isBuy: Boolean): List<NewLimitOrder> {
        return orders["$asset-$isBuy"]?:LinkedList<NewLimitOrder>()
    }

    fun getLastOrder(asset: String, isBuy: Boolean): NewLimitOrder? {
        return orders["$asset-$isBuy"]?.last()
    }
}