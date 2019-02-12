package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.Tick
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.CandlesDatabaseAccessor
import com.lykke.matching.engine.database.HoursCandlesDatabaseAccessor
import com.lykke.utils.logging.PerformanceLogger
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.Date
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.BlockingQueue
import kotlin.concurrent.thread

class TradesInfoService(private val candlesDatabaseAccessor: CandlesDatabaseAccessor?,
                        private val hoursCandlesDatabaseAccessor: HoursCandlesDatabaseAccessor?,
                        private val tradeInfoQueue: BlockingQueue<TradeInfo>) {

    private val formatter = SimpleDateFormat("yyyyMMddHHmm")

    private val isCandlesEnabled = candlesDatabaseAccessor != null
    private val candles = HashMap<String, HashMap<String, HashMap<Int, Tick>>>()

    private val isHoursCandlesEnabled = hoursCandlesDatabaseAccessor != null
    private val savedHoursCandles = hoursCandlesDatabaseAccessor?.getHoursCandles()
    private val hoursCandles = HashMap<String, BigDecimal>()

    private val bid = "Bid"
    private val ask = "Ask"

    private val candlesPerformanceLogger = PerformanceLogger(LoggerFactory.getLogger("historyPersistStats"), 1, "saveCandles: ")
    private val hourCandlesPerformanceLogger = PerformanceLogger(LoggerFactory.getLogger("historyPersistStats"), 1, "saveHourCandles: ")

    fun start() {
        thread(start = true, name = TradesInfoService::class.java.name) {
            while (true) {
                process(tradeInfoQueue.take())
            }
        }
    }

    private fun process(info: TradeInfo) {
        if (isCandlesEnabled) {
            addCandles(info)
        }
        if (isHoursCandlesEnabled) {
            addHoursCandles(info)
        }
    }

    private fun addCandles(info: TradeInfo) {
        val asset = "${info.assetPair}_${if (info.isBuy) bid else ask}"
        val dateTime = LocalDateTime.ofInstant(info.date.toInstant(), ZoneId.of("UTC"))
        val time = formatter.format(info.date)
        val second = dateTime.second
        synchronized(candles) {
            val map = candles.getOrPut(time) { HashMap() }
            val ticks = map.getOrPut(asset) { HashMap() }
            val curTick = ticks[second]
            if (curTick != null) {
                if (curTick.highPrice < info.price) curTick.highPrice = info.price
                if (curTick.lowPrice > info.price) curTick.lowPrice = info.price
                curTick.closePrice = info.price
            } else {
                ticks[second] = Tick(info.price, info.price, info.price, info.price)
            }
        }
    }

    private fun addHoursCandles(info: TradeInfo) {
        if (!info.isBuy || !hoursCandles.containsKey(info.assetPair)) {
            synchronized(hoursCandles) {
                hoursCandles.put(info.assetPair, info.price)
            }
        }
    }

    fun saveCandles() {
        if (!isCandlesEnabled) {
            return
        }
        candlesDatabaseAccessor!!

        candlesPerformanceLogger.start()
        val time = formatter.format(Date())
        synchronized(candles) {
            val keys = candles.keys.filter { it != time }
            candlesPerformanceLogger.startPersist()
            keys.forEach { keyTime ->
                candles[keyTime]?.forEach { asset ->
                    candlesDatabaseAccessor.writeCandle(Candle(asset.key, keyTime, asset.value.entries.joinToString("|")
                    { "O=${it.value.openPrice};C=${it.value.closePrice};H=${it.value.highPrice};L=${it.value.lowPrice};T=${it.key}" }))
                }
                candles.remove(keyTime)
            }
            candlesPerformanceLogger.endPersist()
        }
        candlesPerformanceLogger.end()
        candlesPerformanceLogger.fixTime()
    }

    fun saveHourCandles() {
        if (!isHoursCandlesEnabled) {
            return
        }
        hoursCandlesDatabaseAccessor!!
        savedHoursCandles!!

        hourCandlesPerformanceLogger.start()
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
        hourCandlesPerformanceLogger.startPersist()
        hoursCandlesDatabaseAccessor.writeHourCandles(savedHoursCandles)
        hourCandlesPerformanceLogger.endPersist()
        hourCandlesPerformanceLogger.end()
        hourCandlesPerformanceLogger.fixTime()
    }
}