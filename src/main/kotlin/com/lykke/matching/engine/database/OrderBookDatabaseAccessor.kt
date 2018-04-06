package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.NewLimitOrder
import java.util.concurrent.PriorityBlockingQueue

interface OrderBookDatabaseAccessor {
    fun loadLimitOrders(): List<NewLimitOrder>
    fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: PriorityBlockingQueue<NewLimitOrder>)
}