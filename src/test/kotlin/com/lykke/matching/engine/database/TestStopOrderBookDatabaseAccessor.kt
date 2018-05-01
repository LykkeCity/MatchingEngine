package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.NewLimitOrder
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList

class TestStopOrderBookDatabaseAccessor: StopOrderBookDatabaseAccessor {

    private val stopOrders = HashMap<String, MutableList<NewLimitOrder>>()

    override fun loadStopLimitOrders(): List<NewLimitOrder> {
        val result = LinkedList<NewLimitOrder>()
        stopOrders.values.forEach { result.addAll(it.map { copyOfNewLimitOrder(it) }) }
        return result
    }

    override fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>) {
        stopOrders["$assetPairId-$isBuy"] = orderBook.map { copyOfNewLimitOrder(it) }.toMutableList()
    }

    fun addStopLimitOrder(order: NewLimitOrder) {
        stopOrders.getOrPut("${order.assetPairId}-${order.isBuySide()}") { ArrayList() }.add(copyOfNewLimitOrder(order))
    }

    fun getStopOrders(asset: String, isBuy: Boolean): List<NewLimitOrder> {
        return (stopOrders["$asset-$isBuy"] ?: LinkedList()).map { copyOfNewLimitOrder(it) }
    }

    fun clear() {
        stopOrders.clear()
    }

    fun copyOfNewLimitOrder(order: NewLimitOrder) = TestFileOrderDatabaseAccessor.copyOfNewLimitOrder(order)
}