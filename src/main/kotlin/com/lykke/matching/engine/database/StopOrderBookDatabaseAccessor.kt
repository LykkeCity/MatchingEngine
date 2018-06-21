package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LimitOrder

interface StopOrderBookDatabaseAccessor {
    fun loadStopLimitOrders(): List<LimitOrder>
    fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<LimitOrder>)
}