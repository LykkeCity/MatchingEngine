package com.lykke.matching.engine.performance

import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct
import kotlin.concurrent.fixedRateTimer

@Component
@Profile("default")
class PerformanceStatsLoggerStarter @Autowired constructor(private val performanceStatsLogger: PerformanceStatsLogger,
                                                           private val performanceStatsHolder: PerformanceStatsHolder,
                                                           private val config: Config) {
    @PostConstruct
    fun start() {
        fixedRateTimer(name = "PerformanceStatsLogger", initialDelay = config.me.performanceStatsInterval, period = config.me.performanceStatsInterval) {
            performanceStatsLogger.logStats(performanceStatsHolder.getStatsAndReset().values)
        }
    }
}