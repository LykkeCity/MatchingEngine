package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle

interface LimitOrderDatabaseAccessor {
    fun updateBestPrices(prices: List<BestPrice>)
    fun writeCandle(candle: Candle)

    fun getHoursCandles(): MutableList<HourCandle>
    fun writeHourCandles(candles: List<HourCandle>)
}