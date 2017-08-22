package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

class TestFileOrderDatabaseAccessor: OrderBookDatabaseAccessor {

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: PriorityBlockingQueue<LimitOrder>) {
        orders["$asset-$isBuy"] = orderBook.toMutableList()
    }

    private val orders = HashMap<String, MutableList<LimitOrder>>()

    fun addLimitOrder(order: LimitOrder) {
        orders.getOrPut("${order.assetPairId}-${order.isBuySide()}") {ArrayList()}.add(order)
    }
    override fun loadLimitOrders(): List<LimitOrder> {
        val result = LinkedList<LimitOrder>()
        orders.values.forEach { result.addAll(it) }
        return result
    }

    fun clear() {
        orders.clear()
    }

    fun getOrders(asset: String, isBuy: Boolean): List<LimitOrder> {
        return orders["$asset-$isBuy"]?:LinkedList<LimitOrder>()
    }

    fun getLastOrder(asset: String, isBuy: Boolean): LimitOrder? {
        return orders["$asset-$isBuy"]?.last()
    }
}