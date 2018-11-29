package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.HourCandle

interface HoursCandlesDatabaseAccessor {
    fun getHoursCandles(): MutableList<HourCandle>
    fun writeHourCandles(candles: List<HourCandle>)
}