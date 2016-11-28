package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.LimitOrder
import java.text.SimpleDateFormat
import java.util.ArrayList

class TestLimitOrderDatabaseAccessor : LimitOrderDatabaseAccessor {

    val DATE_FORMAT = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val orders = ArrayList<LimitOrder>()
    val ordersDone = ArrayList<LimitOrder>()
    var bestPrices: List<BestPrice> = ArrayList()
    var candles = ArrayList<Candle>()
    var hoursCandles = ArrayList<HourCandle>()

    override fun loadLimitOrders(): List<LimitOrder> {
        return orders
    }

    override fun addLimitOrder(order: LimitOrder) {
        orders.add(order)
    }

    override fun addLimitOrders(orders: List<LimitOrder>) {
        this.orders.addAll(orders)
    }

    override fun updateLimitOrder(order: LimitOrder) {
        //nothing to do, already in memory
    }

    override fun deleteLimitOrders(orders: List<LimitOrder>) {
        this.orders.removeAll(orders)
    }

    override fun addLimitOrderDone(order: LimitOrder) {
        ordersDone.add(order)
    }

    override fun addLimitOrdersDone(orders: List<LimitOrder>) {
        ordersDone.addAll(orders)
    }

    override fun addLimitOrderDoneWithGeneratedRowId(order: LimitOrder) {
    }

    fun getLastOrder() = orders.last()

    fun clear() = {
        orders.clear()
        ordersDone.clear()
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