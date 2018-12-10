package com.lykke.matching.engine.history

import com.lykke.matching.engine.database.BestPriceDatabaseAccessor
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.springframework.scheduling.annotation.Scheduled

class BestPriceUpdater(private val genericLimitOrderService: GenericLimitOrderService,
                       private val bestPriceDatabaseAccessor: BestPriceDatabaseAccessor) {

    @Scheduled(fixedRateString = "#{Config.me.bestPricesInterval}")
    fun update() {
        bestPriceDatabaseAccessor.updateBestPrices(genericLimitOrderService.buildMarketProfile())
    }
}