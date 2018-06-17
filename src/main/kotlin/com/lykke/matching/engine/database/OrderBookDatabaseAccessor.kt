package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.NewLimitOrder

interface OrderBookDatabaseAccessor {
    fun loadLimitOrders(): List<NewLimitOrder>
    fun updateOrderBook(asset: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>)
}