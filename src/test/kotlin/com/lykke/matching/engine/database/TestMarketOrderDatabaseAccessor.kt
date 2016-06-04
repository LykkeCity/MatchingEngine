package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MatchingData
import com.lykke.matching.engine.daos.OrderTradesLink
import com.lykke.matching.engine.daos.Trade
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Date

class TestMarketOrderDatabaseAccessor : MarketOrderDatabaseAccessor {

    val DATE_FORMAT = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val orders = ArrayList<MarketOrder>()
    val trades = ArrayList<Trade>()
    val matchingData = ArrayList<MatchingData>()
    val orderTradesLinks = ArrayList<OrderTradesLink>()

    override fun addMarketOrder(order: MarketOrder) {
        orders.add(order)
    }

    override fun addMarketOrderWithGeneratedRowId(order: MarketOrder) {
        val orderClientTimeKey = MarketOrder(Date().time.toString(), order.assetPairId, order.clientId, order.volume, order.price,
                order.status, order.createdAt, order.registered, null, null, order.straight)
        orderClientTimeKey.partitionKey = order.clientId
        orderClientTimeKey.rowKey = "%s.#%02d".format(DATE_FORMAT.format(order.matchedAt), 0)
        orders.add(orderClientTimeKey)
    }

    override fun updateMarketOrder(order: MarketOrder) {
        //nothing to do, already in memory
    }

    override fun addTrades(trades: List<Trade>) {
        this.trades.addAll(trades)
    }

    override fun addMatchingData(data: List<MatchingData>) {
        this.matchingData.addAll(data)
    }

    override fun addOrderTradesLinks(links: List<OrderTradesLink>) {
        this.orderTradesLinks.addAll(links)
    }

    fun getLastOrder() = orders.last()

    fun clear() = {
        orders.clear()
        trades.clear()
        matchingData.clear()
        orderTradesLinks.clear()
    }
}