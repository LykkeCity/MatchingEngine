package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList

class TestStopOrderBookDatabaseAccessor: StopOrderBookDatabaseAccessor {

    private val stopOrders = HashMap<String, MutableList<LimitOrder>>()

    override fun loadStopLimitOrders(): List<LimitOrder> {
        val result = LinkedList<LimitOrder>()
        stopOrders.values.forEach { result.addAll(it.map { copyOfNewLimitOrder(it) }) }
        return result
    }

    override fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        stopOrders["$assetPairId-$isBuy"] = orderBook.map { copyOfNewLimitOrder(it) }.toMutableList()
    }

    fun addStopLimitOrder(order: LimitOrder) {
        stopOrders.getOrPut("${order.assetPairId}-${order.isBuySide()}") { ArrayList() }.add(copyOfNewLimitOrder(order))
    }

    fun getStopOrders(asset: String, isBuy: Boolean): List<LimitOrder> {
        return (stopOrders["$asset-$isBuy"] ?: LinkedList()).map { copyOfNewLimitOrder(it) }
    }

    fun clear() {
        stopOrders.clear()
    }

    fun copyOfNewLimitOrder(order: LimitOrder) = TestFileOrderDatabaseAccessor.copyOfNewLimitOrder(order)
}