package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.Tick
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.Date
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.BlockingQueue

class TradesInfoService(private val tradesInfoQueue: BlockingQueue<TradeInfo>,
                        private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor): Thread() {

    companion object {
        val LOGGER = Logger.getLogger(TradesInfoService::class.java.name)
    }

    val formatter = SimpleDateFormat("yyyyMMddHHmm")

    val hourFormatter = SimpleDateFormat("yyyyMMddHH")

    val candles = HashMap<String, HashMap<String, HashMap<Int, Tick>>>()
    val savedHoursCandles = limitOrderDatabaseAccessor.getHoursCandles()
    val hoursCandles = HashMap<String, Double>()

    val bid = "Bid"
    val ask = "Ask"

    override fun run() {
        while (true) {
            process(tradesInfoQueue.take())
        }
    }

    fun process(info: TradeInfo) {
        val asset = "${info.assetPair}_${if (info.isBuy) bid else ask}"
        val dateTime = LocalDateTime.ofInstant(info.date.toInstant(), ZoneId.of("UTC"))
        val time = formatter.format(info.date)
        val second = dateTime.second

        synchronized(candles) {
            val map = candles.getOrPut(time) { HashMap<String, HashMap<Int, Tick>>() }
            val ticks = map.getOrPut(asset) { HashMap<Int, Tick>() }
            val curTick = ticks[second]
            if (curTick != null) {
                if (curTick.highPrice < info.price) curTick.highPrice = info.price
                if (curTick.lowPrice > info.price) curTick.lowPrice = info.price
                curTick.closePrice = info.price
            } else {
                ticks[second] = Tick(info.price, info.price, info.price, info.price)
            }
        }

        if (!info.isBuy || !hoursCandles.containsKey(info.assetPair)) {
            synchronized(hoursCandles) {
                hoursCandles.put(info.assetPair, info.price)
            }
        }
    }

    fun saveCandles() {
        val time = formatter.format(Date())
        synchronized(candles) {
            val keys = candles.keys.filter { it != time }
            keys.forEach { keyTime ->
                candles[keyTime]?.forEach { asset ->
                    limitOrderDatabaseAccessor.writeCandle(Candle(asset.key, keyTime, asset.value.entries.joinToString("|")
                    { "O=${it.value.openPrice};C=${it.value.closePrice};H=${it.value.highPrice};L=${it.value.lowPrice};T=${it.key}" }))
                }
                candles.remove(keyTime)
            }
        }
    }

    fun saveHourCandles() {
        synchronized(hoursCandles) {
            hoursCandles.keys.forEach { asset ->
                var hourCandle = savedHoursCandles.find { it.asset == asset }
                if (hourCandle == null) {
                    hourCandle = HourCandle(asset, LinkedList())
                    hourCandle.addPrice(hoursCandles[asset])
                    savedHoursCandles.add(hourCandle)
                } else {
                    hourCandle.addPrice(hoursCandles[asset])
                }
            }
            hoursCandles.clear()
        }
        limitOrderDatabaseAccessor.writeHourCandles(savedHoursCandles)
    }
}