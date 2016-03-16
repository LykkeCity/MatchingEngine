package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MatchingData
import com.lykke.matching.engine.daos.OrderTradesLink
import com.lykke.matching.engine.daos.Trade

interface MarketOrderDatabaseAccessor {
    fun addMarketOrder(order: MarketOrder)
    fun addMarketOrderWithGeneratedRowId(order: MarketOrder)
    fun updateMarketOrder(order: MarketOrder)

    fun addTrades(trades: List<Trade>)
    fun addMatchingData(data: List<MatchingData>)
    fun addOrderTradesLinks(links: List<OrderTradesLink>)
}