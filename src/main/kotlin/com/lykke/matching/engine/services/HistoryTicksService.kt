package com.lykke.matching.engine.services

import com.lykke.matching.engine.database.cache.MarketStateCache
import org.springframework.context.ApplicationContext
import java.time.Duration
import java.util.*
import kotlin.concurrent.fixedRateTimer

class HistoryTicksService constructor (applicationContext: ApplicationContext,
        private val genericLimitOrderService: GenericLimitOrderService) {

    private lateinit var historyTicksPersist: Timer

    private var marketStateCache = applicationContext.getBean(MarketStateCache::class.java)
    private val frequency = applicationContext.environment.getProperty("history.ticks.update").toLong()

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
