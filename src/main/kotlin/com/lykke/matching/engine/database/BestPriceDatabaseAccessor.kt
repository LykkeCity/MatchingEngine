package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.BestPrice

interface BestPriceDatabaseAccessor {
    fun updateBestPrices(prices: List<BestPrice>)
}