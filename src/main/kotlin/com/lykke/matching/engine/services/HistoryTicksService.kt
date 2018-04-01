package com.lykke.matching.engine.services

import com.lykke.matching.engine.database.cache.MarketStateCache
import com.lykke.utils.logging.PerformanceLogger
import org.apache.log4j.Logger
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.concurrent.fixedRateTimer

class HistoryTicksService(
        private val marketStateCache: MarketStateCache,
        private val genericLimitOrderService: GenericLimitOrderService,
        private val frequency: Long) {

    private val performanceLogger = PerformanceLogger(Logger.getLogger("historyPersistStats"), 10, "buildTicks: ")
    private lateinit var historyTicksPersist: Timer

    fun start(): Timer {
        historyTicksPersist = fixedRateTimer(name = "HistoryTicksPersist", initialDelay = 0, period = getInterval(Duration.ofHours(1))) {
            marketStateCache.flush()
        }

        return fixedRateTimer(name = "HistoryTicksBuilder", initialDelay = 0, period = getInterval(Duration.ofHours(1))) {
            recordTicks()
        }
    }

    private fun recordTicks() {
        performanceLogger.start()
        val startTimeOfBuildingMarketProfile = Date()
        val bestPrices = genericLimitOrderService.buildMarketProfile()

        bestPrices.forEach {
            marketStateCache.addTick(it.asset, it.ask, it.bid, startTimeOfBuildingMarketProfile.time)
        }
        performanceLogger.end()
        performanceLogger.fixTime()
    }

    private fun getInterval(duration: Duration): Long {
        return duration.get(ChronoUnit.MILLIS) / frequency
    }
}
