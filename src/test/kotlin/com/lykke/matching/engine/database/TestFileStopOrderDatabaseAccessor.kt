package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList


class TestFileStopOrderDatabaseAccessor : StopOrderBookDatabaseAccessor {

    private val orders = HashMap<String, MutableList<LimitOrder>>()

    override fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        orders["$assetPairId-$isBuy"] = orderBook.map { it.copy() }.toMutableList()
    }

    fun addStopLimitOrder(order: LimitOrder) {
        orders.getOrPut("${order.assetPairId}-${order.isBuySide()}") { ArrayList() }.add(order.copy())
    }

    override fun loadStopLimitOrders(): List<LimitOrder> {
        val result = LinkedList<LimitOrder>()
        orders.values.forEach { result.addAll(it.map { it.copy() }) }
        return result
    }

    fun clear() {
        orders.clear()
    }

    fun getOrders(asset: String, isBuy: Boolean): List<LimitOrder> {
        return (orders["$asset-$isBuy"] ?: LinkedList()).map { it.copy() }
    }
}