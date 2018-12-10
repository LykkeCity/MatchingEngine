package com.lykke.matching.engine.history

import com.lykke.matching.engine.services.TradesInfoService
import org.springframework.scheduling.annotation.Scheduled

class HoursCandlesUpdater(private val tradesInfoService: TradesInfoService) {

    @Scheduled(fixedRateString = "#{Config.me.hoursCandleSaverInterval}")
    fun update() {
        tradesInfoService.saveHourCandles()
    }

}