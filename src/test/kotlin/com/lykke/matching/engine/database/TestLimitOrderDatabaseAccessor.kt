package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import java.text.SimpleDateFormat
import java.util.ArrayList

class TestLimitOrderDatabaseAccessor : LimitOrderDatabaseAccessor {

    val DATE_FORMAT = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    var bestPrices: List<BestPrice> = ArrayList()
    var candles = ArrayList<Candle>()
    var hoursCandles = ArrayList<HourCandle>()

    fun clear() {
        candles.clear()
        hoursCandles.clear()
    }

    override fun updateBestPrices(prices: List<BestPrice>) {
        bestPrices = prices
    }

    override fun writeCandle(candle: Candle) {
        candles.add(candle)
    }

    override fun getHoursCandles(): MutableList<HourCandle> {
        return hoursCandles
    }

    override fun writeHourCandles(candles: List<HourCandle>) {
        hoursCandles.clear()
        hoursCandles.addAll(candles)
    }
}