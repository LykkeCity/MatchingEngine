package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.NewLimitOrder
import java.text.SimpleDateFormat
import java.util.ArrayList

class TestLimitOrderDatabaseAccessor : LimitOrderDatabaseAccessor {

    val DATE_FORMAT = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val orders = ArrayList<NewLimitOrder>()
    val ordersDone = ArrayList<NewLimitOrder>()
    var bestPrices: List<BestPrice> = ArrayList()
    var candles = ArrayList<Candle>()
    var hoursCandles = ArrayList<HourCandle>()

    override fun loadLimitOrders(): List<NewLimitOrder> {
        return orders
    }

    override fun addLimitOrder(order: NewLimitOrder) {
        orders.add(order)
    }

    override fun addLimitOrders(orders: List<NewLimitOrder>) {
        this.orders.addAll(orders)
    }

    override fun updateLimitOrder(order: NewLimitOrder) {
        //nothing to do, already in memory
    }

    override fun deleteLimitOrders(orders: List<NewLimitOrder>) {
        this.orders.removeAll(orders)
    }

    override fun addLimitOrderDone(order: NewLimitOrder) {
        ordersDone.add(order)
    }

    override fun addLimitOrdersDone(orders: List<NewLimitOrder>) {
        ordersDone.addAll(orders)
    }

    override fun addLimitOrderDoneWithGeneratedRowId(order: NewLimitOrder) {
    }

    fun getLastOrder() = orders.last()

    fun clear() {
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