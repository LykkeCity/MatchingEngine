package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.BestPriceDatabaseAccessor
import com.lykke.matching.engine.database.CandlesDatabaseAccessor
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.HoursCandlesDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureBestPriceDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureCandlesDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureHistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureHoursCandlesDatabaseAccessor
import com.lykke.matching.engine.database.cache.MarketStateCache
import com.lykke.matching.engine.history.BestPriceUpdater
import com.lykke.matching.engine.history.CandlesUpdater
import com.lykke.matching.engine.history.HoursCandlesUpdater
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.HistoryTicksService
import com.lykke.matching.engine.services.TradesInfoService
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.TaskScheduler
import java.util.Optional
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
@Profile("default")
open class MarketHistoryConfig {

    companion object {
        private const val PROP_NAME_TICK_FREQUENCY = "application.tick.frequency"
        private const val PROP_NAME_BEST_PRICE_TABLE = "azure.best.price.table"
        private const val PROP_NAME_CANDLES_TABLE = "azure.candles.table"
        private const val PROP_NAME_HOUR_CANDLES_TABLE = "azure.hour.candles.table"
    }

    @Autowired
    private lateinit var config: Config

    // <editor-fold desc="Best Price">

    @Bean
    open fun azureBestPriceDatabaseAccessor(@Value("\${$PROP_NAME_BEST_PRICE_TABLE:#{null}}")
                                            bestPricesTable: String?): AzureBestPriceDatabaseAccessor? {
        if (config.me.disableBestPriceHistory == true) {
            return null
        }
        checkMandatoryValue(bestPricesTable, PROP_NAME_BEST_PRICE_TABLE)
        return AzureBestPriceDatabaseAccessor(config.me.db.hBestPriceConnString, bestPricesTable!!)
    }

    @Bean
    open fun bestPriceUpdater(genericLimitOrderService: GenericLimitOrderService,
                              bestPriceDatabaseAccessor: Optional<BestPriceDatabaseAccessor>): BestPriceUpdater? {
        if (config.me.disableBestPriceHistory == true) {
            return null
        }
        return BestPriceUpdater(genericLimitOrderService, bestPriceDatabaseAccessor.get())
    }

    // </editor-fold>

    // <editor-fold desc="Candles">

    @Bean
    open fun azureCandlesDatabaseAccessor(@Value("\${$PROP_NAME_CANDLES_TABLE:#{null}}")
                                          candlesTable: String?): AzureCandlesDatabaseAccessor? {
        if (config.me.disableCandlesHistory == true) {
            return null
        }
        checkMandatoryValue(candlesTable, PROP_NAME_CANDLES_TABLE)
        return AzureCandlesDatabaseAccessor(config.me.db.hCandlesConnString, candlesTable!!)
    }

    @Bean
    open fun azureHoursCandlesDatabaseAccessor(@Value("\${$PROP_NAME_HOUR_CANDLES_TABLE:#{null}}")
                                               hourCandlesTable: String?): AzureHoursCandlesDatabaseAccessor? {
        if (config.me.disableHourCandlesHistory == true) {
            return null
        }
        checkMandatoryValue(hourCandlesTable, PROP_NAME_HOUR_CANDLES_TABLE)
        return AzureHoursCandlesDatabaseAccessor(config.me.db.hHourCandlesConnString, hourCandlesTable!!)
    }

    @Bean(initMethod = "start")
    open fun tradesInfoService(candlesDatabaseAccessor: Optional<CandlesDatabaseAccessor>,
                               hoursCandlesDatabaseAccessor: Optional<HoursCandlesDatabaseAccessor>,
                               tradeInfoQueue: Optional<BlockingQueue<TradeInfo>>): TradesInfoService? {
        if (config.me.disableCandlesHistory == true && config.me.disableHourCandlesHistory == true) {
            return null
        }

        return TradesInfoService(if (config.me.disableCandlesHistory != true) candlesDatabaseAccessor.get() else null,
                if (config.me.disableHourCandlesHistory != true) hoursCandlesDatabaseAccessor.get() else null,
                tradeInfoQueue.get())
    }

    @Bean
    open fun tradeInfoQueue(): BlockingQueue<TradeInfo>? {
        if (config.me.disableCandlesHistory == true && config.me.disableHourCandlesHistory == true) {
            return null
        }
        return LinkedBlockingQueue<TradeInfo>()
    }

    @Bean(initMethod = "start")
    open fun candlesUpdater(tradesInfoService: Optional<TradesInfoService>,
                            taskScheduler: TaskScheduler): CandlesUpdater? {
        if (config.me.disableCandlesHistory == true) {
            return null
        }
        return CandlesUpdater(tradesInfoService.get(),
                taskScheduler,
                config.me.candleSaverInterval)
    }

    @Bean
    open fun hoursCandlesUpdater(tradesInfoService: Optional<TradesInfoService>): HoursCandlesUpdater? {
        if (config.me.disableHourCandlesHistory == true) {
            return null
        }
        return HoursCandlesUpdater(tradesInfoService.get())
    }

    // </editor-fold>

    // <editor-fold desc="History">

    @Bean
    open fun azureHistoryTicksDatabaseAccessor(@Value("\${$PROP_NAME_TICK_FREQUENCY:#{null}}")
                                               frequency: Long?)
            : HistoryTicksDatabaseAccessor? {
        if (config.me.disableBlobHistory == true) {
            return null
        }
        checkMandatoryValue(frequency, PROP_NAME_TICK_FREQUENCY)
        return AzureHistoryTicksDatabaseAccessor(config.me.db.hBlobConnString, frequency!!)
    }

    @Bean
    open fun marketStateCache(historyTicksDatabaseAccessor: Optional<HistoryTicksDatabaseAccessor>,
                              @Value("\${$PROP_NAME_TICK_FREQUENCY:#{null}}")
                              frequency: Long?): MarketStateCache? {
        if (config.me.disableBlobHistory == true) {
            return null
        }
        checkMandatoryValue(frequency, PROP_NAME_TICK_FREQUENCY)
        return MarketStateCache(historyTicksDatabaseAccessor.get(), frequency!!)
    }

    @Bean(initMethod = "start")
    open fun historyTicksService(genericLimitOrderService: GenericLimitOrderService,
                                 marketStateCache: Optional<MarketStateCache>,
                                 @Value("\${$PROP_NAME_TICK_FREQUENCY:#{null}}")
                                 frequency: Long?): HistoryTicksService? {
        if (config.me.disableBlobHistory == true) {
            return null
        }
        checkMandatoryValue(frequency, PROP_NAME_TICK_FREQUENCY)
        return HistoryTicksService(marketStateCache.get(),
                genericLimitOrderService,
                frequency!!)
    }

    // </editor-fold>

    private fun checkMandatoryValue(value: Any?, name: String) {
        if (value == null) {
            throw IllegalStateException("App property '$name' is absent")
        }
    }
}