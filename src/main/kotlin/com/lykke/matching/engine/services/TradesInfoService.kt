package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.Tick
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.outgoing.rabbit.events.TradeInfoEvent
import com.lykke.utils.logging.PerformanceLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.Date
import java.util.HashMap
import java.util.LinkedList
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

@Component
class TradesInfoService @Autowired constructor(private val limitOrderDatabaseAccessor: LimitOrderDatabaseAccessor) {

    companion object {
        val LOGGER = Logger.getLogger(TradesInfoService::class.java.name)
    }

    private val tradeInfoQueue = LinkedBlockingQueue<TradeInfo>()

    val formatter = SimpleDateFormat("yyyyMMddHHmm")

    val hourFormatter = SimpleDateFormat("yyyyMMddHH")

    val candles = HashMap<String, HashMap<String, HashMap<Int, Tick>>>()
    val savedHoursCandles = limitOrderDatabaseAccessor.getHoursCandles()
    val hoursCandles = HashMap<String, BigDecimal>()

    val bid = "Bid"
    val ask = "Ask"

    private val candlesPerformanceLogger = PerformanceLogger(Logger.getLogger("historyPersistStats"), 1, "saveCandles: ")
    private val hourCandlesPerformanceLogger = PerformanceLogger(Logger.getLogger("historyPersistStats"), 1, "saveHourCandles: ")


    @PostConstruct
    fun initialize() {
        thread(start = true, name = TradesInfoService::class.java.name) {
            while (true) {
                process(tradeInfoQueue.take())
            }
        }
    }

    @EventListener
    fun processEvent(infoEvent: TradeInfoEvent) {
        tradeInfoQueue.add(infoEvent.tradeInfo)
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
        candlesPerformanceLogger.start()
        val time = formatter.format(Date())
        synchronized(candles) {
            val keys = candles.keys.filter { it != time }
            candlesPerformanceLogger.startPersist()
            keys.forEach { keyTime ->
                candles[keyTime]?.forEach { asset ->
                    limitOrderDatabaseAccessor.writeCandle(Candle(asset.key, keyTime, asset.value.entries.joinToString("|")
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
        limitOrderDatabaseAccessor.writeHourCandles(savedHoursCandles)
        hourCandlesPerformanceLogger.endPersist()
        hourCandlesPerformanceLogger.end()
        hourCandlesPerformanceLogger.fixTime()
    }
}