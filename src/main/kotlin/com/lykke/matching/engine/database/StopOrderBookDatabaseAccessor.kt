package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.NewLimitOrder

interface StopOrderBookDatabaseAccessor {
    fun loadStopLimitOrders(): List<NewLimitOrder>
    fun updateStopOrderBook(assetPairId: String, isBuy: Boolean, orderBook: Collection<NewLimitOrder>)
}