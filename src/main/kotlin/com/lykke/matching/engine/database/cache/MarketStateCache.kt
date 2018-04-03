package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.TickUpdateInterval
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.history.TickBlobHolder
import com.lykke.utils.logging.PerformanceLogger
import org.apache.log4j.Logger
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors
import kotlin.collections.HashMap

class MarketStateCache(private val historyTicksDatabaseAccessor: HistoryTicksDatabaseAccessor,
                       private val frequency: Long) {

    companion object {
        private val performanceLogger = PerformanceLogger(Logger.getLogger("marketStateCache"), 10, "buildTicks: ")
    }

    private val assetPairToIntervalTickHolder = HashMap<String, HashMap<TickUpdateInterval, TickBlobHolder>>()
    private val dirtyTicks = LinkedList<TickBlobHolder>()
    init {
        refresh()
    }

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
                dirtyTicks.add(tickBlobHolder)
                return@forEach
            }
            if(isTimeForAddNewTick(blobHolder, currentUpdateTime)) {
                blobHolder.addPrice(ask, bid, currentUpdateTime)
                dirtyTicks.add(blobHolder)
            }
        })
    }

    fun refresh() {
        val ticks = historyTicksDatabaseAccessor.loadHistoryTicks()
        ticks.forEach {
            val assetPairToTick = assetPairToIntervalTickHolder.getOrPut(it.assetPair) {HashMap()}
            assetPairToTick!![it.tickUpdateInterval] = it
        }
    }

    fun flush() {
        performanceLogger.startPersist()

        val ticksToPersist = getTicksToPersist()
        ticksToPersist.forEach(Consumer {
            historyTicksDatabaseAccessor.saveHistoryTick(it)
        })

        performanceLogger.endPersist()
    }

    @Synchronized
    private fun getTicksToPersist(): List<TickBlobHolder> {
        val ticks = dirtyTicks.stream()
                .map { TickBlobHolder(it) }
                .collect(Collectors.toList())

        dirtyTicks.clear()
        return ticks
    }

    private fun getUpdateInterval(tickUpdateInterval: TickUpdateInterval): Long {
        return tickUpdateInterval.interval / frequency
    }

    private fun isTimeForAddNewTick(tickBlobHolder: TickBlobHolder, currentUpdateTime: Long): Boolean {
        return getUpdateInterval(tickBlobHolder.tickUpdateInterval) +  tickBlobHolder.lastUpdate <= currentUpdateTime
    }
}