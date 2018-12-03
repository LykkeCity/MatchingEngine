package com.lykke.matching.engine.performance

import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.TaskScheduler
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.ZonedDateTime
import javax.annotation.PostConstruct

@Component
@Profile("default")
class PerformanceStatsLoggerStarter @Autowired constructor(private val performanceStatsLogger: PerformanceStatsLogger,
                                                           private val performanceStatsHolder: PerformanceStatsHolder,
                                                           private val taskScheduler: TaskScheduler,
                                                           private val config: Config) {
    @PostConstruct
    fun start() {
        taskScheduler.scheduleAtFixedRate({
            Thread.currentThread().name = "PerformanceStatsLogger"
            performanceStatsLogger.logStats(performanceStatsHolder.getStatsAndReset().values)
        }, ZonedDateTime.now().toInstant().plusMillis(config.me.performanceStatsInterval),
                Duration.ofMillis(config.me.performanceStatsInterval))
    }
}