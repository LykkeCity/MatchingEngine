package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.NewLimitOrder

interface LimitOrderDatabaseAccessor {
    fun loadLimitOrders(): List<NewLimitOrder>
    fun addLimitOrder(order: NewLimitOrder)
    fun addLimitOrders(orders: List<NewLimitOrder>)
    fun updateLimitOrder(order: NewLimitOrder)
    fun deleteLimitOrders(orders: List<NewLimitOrder>)

    fun addLimitOrderDone(order: NewLimitOrder)
    fun addLimitOrdersDone(orders: List<NewLimitOrder>)
    fun addLimitOrderDoneWithGeneratedRowId(order: NewLimitOrder)

    fun updateBestPrices(prices: List<BestPrice>)
    fun writeCandle(candle: Candle)

    fun getHoursCandles(): MutableList<HourCandle>
    fun writeHourCandles(candles: List<HourCandle>)
}