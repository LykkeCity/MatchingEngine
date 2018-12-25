package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedList

class TestStopOrderBookDatabaseAccessor(private val secondaryDbAccessor: TestFileStopOrderDatabaseAccessor): StopOrderBookDatabaseAccessor {

    private val stopOrders = HashMap<String, LimitOrder>()

    override fun loadStopLimitOrders(): List<LimitOrder> {
        return stopOrders.values.map { it.copy() }
    }

    override fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<LimitOrder>) {
        // to do nothing
    }

    fun updateOrders(ordersToSave: Collection<LimitOrder>, ordersToRemove: Collection<LimitOrder>) {
        ordersToRemove.forEach { stopOrders.remove(it.id) }
        ordersToSave.forEach { stopOrders[it.id] = it.copy() }
    }

    fun getStopOrders(assetPairId: String, isBuySide: Boolean): List<LimitOrder> {
        return stopOrders.values.filter { it.assetPairId == assetPairId && it.isBuySide() == isBuySide }
    }

    fun addStopLimitOrder(order: LimitOrder) {
        stopOrders[order.id] = order
        secondaryDbAccessor.addStopLimitOrder(order)
    }
}