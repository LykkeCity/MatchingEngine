package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.PriorityBlockingQueue

class TestFileOrderDatabaseAccessor : OrderBookDatabaseAccessor {

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: PriorityBlockingQueue<NewLimitOrder>) {
        orders["$asset-$isBuy"] = orderBook.map { copyOfNewLimitOrder(it) }.toMutableList()
    }

    private val orders = HashMap<String, MutableList<NewLimitOrder>>()

    fun addLimitOrder(order: NewLimitOrder) {
        orders.getOrPut("${order.assetPairId}-${order.isBuySide()}") { ArrayList() }.add(copyOfNewLimitOrder(order))
    }

    override fun loadLimitOrders(): List<NewLimitOrder> {
        val result = LinkedList<NewLimitOrder>()
        orders.values.forEach { result.addAll(it.map { copyOfNewLimitOrder(it) }) }
        return result
    }

    fun clear() {
        orders.clear()
    }

    fun getOrders(asset: String, isBuy: Boolean): List<NewLimitOrder> {
        return (orders["$asset-$isBuy"] ?: LinkedList()).map { copyOfNewLimitOrder(it) }
    }

    fun getLastOrder(asset: String, isBuy: Boolean): NewLimitOrder? {
        return copyOfNewLimitOrder(orders["$asset-$isBuy"]?.last() ?: return null)
    }

    private fun copyOfNewLimitOrder(order: NewLimitOrder): NewLimitOrder {
        return NewLimitOrder(order.id,
                order.externalId,
                order.assetPairId,
                order.clientId,
                order.volume,
                order.price,
                order.status,
                order.createdAt,
                order.registered,
                order.remainingVolume,
                order.lastMatchTime,
                order.reservedLimitVolume,
                order.fee as? LimitOrderFeeInstruction,
                order.fees as? List<NewLimitOrderFeeInstruction>)
    }
}