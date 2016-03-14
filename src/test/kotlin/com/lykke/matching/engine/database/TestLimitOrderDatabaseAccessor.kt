package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Date

class TestLimitOrderDatabaseAccessor : LimitOrderDatabaseAccessor {

    val DATE_FORMAT = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val orders = ArrayList<LimitOrder>()
    val ordersDone = ArrayList<LimitOrder>()

    override fun loadLimitOrders(): List<LimitOrder> {
        return orders
    }

    override fun addLimitOrder(order: LimitOrder) {
        orders.add(order)
    }

    override fun updateLimitOrder(order: LimitOrder) {
        //nothing to do, already in memory
    }

    override fun deleteLimitOrders(orders: List<LimitOrder>) {
        this.orders.removeAll(orders)
    }

    override fun addLimitOrderDone(order: LimitOrder) {
        ordersDone.add(order)
    }

    override fun addLimitOrderDoneWithGeneratedRowId(order: LimitOrder) {
        val orderClientTimeKey = LimitOrder(
                uid = Date().time.toString(),
                assetPairId = order.assetPairId,
                clientId = order.clientId,
                price = order.price,
                createdAt = order.createdAt,
                registered = order.registered,
                status = order.status,
                volume = order.volume,
                remainingVolume = order.volume,
                lastMatchTime = order.lastMatchTime
        )
        orderClientTimeKey.partitionKey = order.clientId
        orderClientTimeKey.rowKey = "%s.#%02d".format(DATE_FORMAT.format(order.lastMatchTime), 0)
        ordersDone.add(orderClientTimeKey)
    }

    fun getLastOrder() = orders.last()

    fun clear() = {
        orders.clear()
        ordersDone.clear()
    }
}