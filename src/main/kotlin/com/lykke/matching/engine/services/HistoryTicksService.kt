package com.lykke.matching.engine.services

import com.lykke.matching.engine.database.cache.MarketStateCache
import com.lykke.utils.logging.PerformanceLogger
import org.apache.log4j.Logger
import java.time.Duration
import java.util.*
import kotlin.concurrent.fixedRateTimer

class HistoryTicksService(
        private val marketStateCache: MarketStateCache,
        private val genericLimitOrderService: GenericLimitOrderService,
        private val frequency: Long) {

    private lateinit var historyTicksPersist: Timer

    fun start(): Timer {
        val persistPeriod = getPeriod(Duration.ofDays(1))
        historyTicksPersist = fixedRateTimer(name = "HistoryTicksPersist", initialDelay = persistPeriod, period = persistPeriod) {
            marketStateCache.flush()
        }

        return fixedRateTimer(name = "HistoryTicksBuilder", initialDelay = 0, period = getPeriod(Duration.ofHours(1))) {
            recordTicks()
        }
    }

    private fun recordTicks() {
        val startTimeOfBuildingMarketProfile = Date()
        val bestPrices = genericLimitOrderService.buildMarketProfile()

        bestPrices.forEach {
            marketStateCache.addTick(it.asset, it.ask, it.bid, startTimeOfBuildingMarketProfile.time)
        }
    }

    private fun getPeriod(duration: Duration): Long {
        return duration.toMillis() / frequency
    }
}
