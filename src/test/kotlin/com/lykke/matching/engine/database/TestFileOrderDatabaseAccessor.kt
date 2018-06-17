package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList

class TestFileOrderDatabaseAccessor : OrderBookDatabaseAccessor {

    private val orders = HashMap<String, MutableList<NewLimitOrder>>()

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>) {
        orders["$asset-$isBuy"] = orderBook.map { copyOfNewLimitOrder(it) }.toMutableList()
    }

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

    companion object {
        fun copyOfNewLimitOrder(order: NewLimitOrder): NewLimitOrder {
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
                    order.fees?.map { it as NewLimitOrderFeeInstruction },
                    order.type,
                    order.lowerLimitPrice,
                    order.lowerPrice,
                    order.upperLimitPrice,
                    order.upperPrice,
                    order.previousExternalId)
        }
    }
}