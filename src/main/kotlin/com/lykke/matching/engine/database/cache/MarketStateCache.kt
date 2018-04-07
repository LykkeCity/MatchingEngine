package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.TickUpdateInterval
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.history.TickBlobHolder
import com.lykke.utils.logging.PerformanceLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.function.Consumer
import java.util.stream.Collectors
import kotlin.collections.HashMap

@Component
class MarketStateCache @Autowired constructor (private val historyTicksDatabaseAccessor: HistoryTicksDatabaseAccessor,
                                                   @Value("\${application.settings.update.interval}") private val frequency: Long) {

    companion object {
        private val performanceLogger = PerformanceLogger(Logger.getLogger("marketStateCache"), 10, "buildTicks: ")
    }

    private val assetPairToIntervalTickHolder = HashMap<String, HashMap<TickUpdateInterval, TickBlobHolder>>()

    @Synchronized
    fun addTick(assetPair: String, ask: Double, bid: Double, currentUpdateTime: Long) {
        val intervalToTickBlobHolder = assetPairToIntervalTickHolder.getOrPut(assetPair) { HashMap() }

        TickUpdateInterval.values().forEach({ interval ->
            val blobHolder = intervalToTickBlobHolder[interval]
            if(blobHolder == null) {
                val tickBlobHolder = historyTicksDatabaseAccessor.loadHistoryTick(assetPair, interval) ?: TickBlobHolder(assetPair = assetPair,
                        tickUpdateInterval = interval,
                        lastUpdate = currentUpdateTime,
                        frequency = frequency)
                intervalToTickBlobHolder[interval] = tickBlobHolder

                tickBlobHolder.addPrice(ask, bid, currentUpdateTime)
                return@forEach
            }
            if(isTimeForAddNewTick(blobHolder, currentUpdateTime)) {
                blobHolder.addPrice(ask, bid, currentUpdateTime)
            }
        })
    }

    fun refresh() {
        val ticks = historyTicksDatabaseAccessor.loadHistoryTicks()
        ticks.forEach {
            val assetPairToTick = assetPairToIntervalTickHolder.getOrPut(it.assetPair) {HashMap()}
            assetPairToTick[it.tickUpdateInterval] = it
        }
    }

    fun flush() {
        performanceLogger.start()
        performanceLogger.startPersist()

        val ticksToPersist = getTicksToPersist()
        ticksToPersist.forEach(Consumer {
            historyTicksDatabaseAccessor.saveHistoryTick(it)
        })

        performanceLogger.endPersist()
        performanceLogger.end()
        performanceLogger.fixTime()
    }

    @Synchronized
    private fun getTicksToPersist(): List<TickBlobHolder> {
        return assetPairToIntervalTickHolder
                .entries
                .stream()
                .flatMap { entry -> entry.value.entries.stream() }
                .map { entry -> TickBlobHolder(entry.value) }
                .collect(Collectors.toList())
    }

    private fun getUpdateInterval(tickUpdateInterval: TickUpdateInterval): Long {
        return tickUpdateInterval.interval / frequency
    }

    private fun isTimeForAddNewTick(tickBlobHolder: TickBlobHolder, currentUpdateTime: Long): Boolean {
        return getUpdateInterval(tickBlobHolder.tickUpdateInterval) +  tickBlobHolder.lastUpdate <= currentUpdateTime
    }
}