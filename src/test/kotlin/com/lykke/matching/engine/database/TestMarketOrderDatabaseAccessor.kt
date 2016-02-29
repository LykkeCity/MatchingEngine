package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.Trade
import java.util.ArrayList

class TestMarketOrderDatabaseAccessor : MarketOrderDatabaseAccessor {

    val orders = ArrayList<MarketOrder>()
    val trades = ArrayList<Trade>()

    override fun addMarketOrder(order: MarketOrder) {
        orders.add(order)
    }

    override fun updateMarketOrder(order: MarketOrder) {
        //nothing to do, already in memory
    }

    override fun addTrades(trades: List<Trade>) {
        this.trades.addAll(trades)
    }

    fun getLastOrder() = orders.last()

    fun clear() = {
        orders.clear()
        trades.clear()
    }
}