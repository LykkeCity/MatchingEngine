package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Candle

interface CandlesDatabaseAccessor {
    fun writeCandle(candle: Candle)
}