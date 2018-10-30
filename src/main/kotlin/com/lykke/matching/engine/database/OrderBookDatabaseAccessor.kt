package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder

interface OrderBookDatabaseAccessor {
    fun loadLimitOrders(): List<LimitOrder>
    fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<LimitOrder>)
}