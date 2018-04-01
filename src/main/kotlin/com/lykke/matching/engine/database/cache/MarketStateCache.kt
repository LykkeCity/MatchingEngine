package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.TickUpdateInterval
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.history.TickBlobHolder
import com.lykke.utils.logging.PerformanceLogger
import org.apache.log4j.Logger
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class MarketStateCache(private val historyTicksDatabaseAccessor: HistoryTicksDatabaseAccessor,
                       private val frequency: Long) {

    companion object {
        private val performanceLogger = PerformanceLogger(Logger.getLogger("marketStateCache"), 10, "buildTicks: ")
    }

    private val assetPairToIntervalTickHolder = HashMap<String, HashMap<TickUpdateInterval, TickBlobHolder>>()
    private val dirtyTicks = ArrayList<TickBlobHolder>()

    init {
        refresh()
    }

    @Synchronized
    fun addTick(assetPair: String, ask: Double, bid: Double, currentUpdateTime: Long) {
        val intervalToTickBlobHolder = assetPairToIntervalTickHolder.getOrPut(assetPair) { HashMap() }

        intervalToTickBlobHolder.forEach({ interval, blobHolder ->
            if(blobHolder == null) {
                val tickBlobHolder = TickBlobHolder(assetPair = assetPair,
                        tickUpdateInterval = interval,
                        askTicks = LinkedList(Arrays.asList(ask)),
                        bidTicks = LinkedList(Arrays.asList(bid)),
                        lastUpdate = currentUpdateTime,
                        frequency = frequency)
                intervalToTickBlobHolder[interval] = tickBlobHolder
                dirtyTicks.add(tickBlobHolder)
            }
            if(isTimeForAddNewTick(blobHolder, currentUpdateTime)) {
                blobHolder.addPrice(ask, bid)
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

        getDirtyTicks().forEach(Consumer {
            historyTicksDatabaseAccessor.saveHistoryTick(it)
        })

        performanceLogger.endPersist()
        dirtyTicks.clear()
    }

    @Synchronized
    private fun getDirtyTicks(): List<TickBlobHolder> {
        return dirtyTicks.stream()
                .map { TickBlobHolder(it) }
                .collect(Collectors.toList())
    }

    private fun getUpdateInterval(tickUpdateInterval: TickUpdateInterval): Long {
        return tickUpdateInterval.period / frequency
    }

    private fun isTimeForAddNewTick(tickBlobHolder: TickBlobHolder, currentUpdateTime: Long): Boolean {
        return getUpdateInterval(tickBlobHolder.tickUpdateInterval) +  tickBlobHolder.lastUpdate <= currentUpdateTime
    }
}