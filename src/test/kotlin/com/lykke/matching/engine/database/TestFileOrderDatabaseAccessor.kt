package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList

class TestFileOrderDatabaseAccessor : OrderBookDatabaseAccessor {

    private val orders = HashMap<String, MutableList<LimitOrder>>()

    override fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        orders["$asset-$isBuy"] = orderBook.map { copyOfNewLimitOrder(it) }.toMutableList()
    }

    fun addLimitOrder(order: LimitOrder) {
        orders.getOrPut("${order.assetPairId}-${order.isBuySide()}") { ArrayList() }.add(copyOfNewLimitOrder(order))
    }

    override fun loadLimitOrders(): List<LimitOrder> {
        val result = LinkedList<LimitOrder>()
        orders.values.forEach { result.addAll(it.map { copyOfNewLimitOrder(it) }) }
        return result
    }

    fun clear() {
        orders.clear()
    }

    fun getOrders(asset: String, isBuy: Boolean): List<LimitOrder> {
        return (orders["$asset-$isBuy"] ?: LinkedList()).map { copyOfNewLimitOrder(it) }
    }

    companion object {
        fun copyOfNewLimitOrder(order: LimitOrder): LimitOrder {
            return LimitOrder(order.id,
                    order.externalId,
                    order.assetPairId,
                    order.clientId,
                    order.volume,
                    order.price,
                    order.status,
                    order.statusDate,
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
                    order.previousExternalId,
                    order.timeInForce,
                    order.expiryTime)
        }
    }
}