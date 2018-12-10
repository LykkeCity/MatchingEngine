package com.lykke.matching.engine.history

import com.lykke.matching.engine.services.TradesInfoService
import org.springframework.scheduling.TaskScheduler
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZonedDateTime

class CandlesUpdater(private val tradesInfoService: TradesInfoService,
                     private val taskScheduler: TaskScheduler,
                     private val interval: Long) {

    fun start() {
        taskScheduler.scheduleAtFixedRate(::update,
                ZonedDateTime.now().toInstant().plusMillis(calculateInitialDelay()),
                Duration.ofMillis(interval))
    }

    private fun update() {
        tradesInfoService.saveCandles()
    }

    private fun calculateInitialDelay(): Long {
        val time = LocalDateTime.now()
        return ((1000 - time.nano / 1000000) + 1000 * (63 - time.second)).toLong()
    }

}