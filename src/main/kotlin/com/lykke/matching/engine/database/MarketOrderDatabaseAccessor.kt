package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LkkTrade

interface MarketOrderDatabaseAccessor {
    fun addLkkTrades(trades: List<LkkTrade>)
}