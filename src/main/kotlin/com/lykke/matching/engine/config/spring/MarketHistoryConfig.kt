package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureHistoryTicksDatabaseAccessor
import com.lykke.matching.engine.database.azure.AzureLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.cache.MarketStateCache
import com.lykke.matching.engine.services.TradesInfoService
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.Optional
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
open class MarketHistoryConfig {

    @Autowired
    private lateinit var config: Config

    @Bean
    open fun azureHistoryTicksDatabaseAccessor(@Value("\${application.tick.frequency:#{null}}")
                                               frequency: Long?)
            : HistoryTicksDatabaseAccessor? {
        if (config.me.disableMarketHistory == true) {
            return null
        }
        return AzureHistoryTicksDatabaseAccessor(config.me.db.hLiquidityConnString, frequency!!)
    }

    @Bean
    open fun azureLimitOrderDatabaseAccessor(@Value("\${azure.best.price.table:#{null}}")
                                             bestPricesTable: String?,
                                             @Value("\${azure.candles.table:#{null}}")
                                             candlesTable: String?,
                                             @Value("\${azure.hour.candles.table:#{null}}")
                                             hourCandlesTable: String?)
            : LimitOrderDatabaseAccessor? {
        if (config.me.disableMarketHistory == true) {
            return null
        }
        return AzureLimitOrderDatabaseAccessor(connectionString = config.me.db.hLiquidityConnString,
                bestPricesTable = bestPricesTable!!, candlesTable = candlesTable!!, hourCandlesTable = hourCandlesTable!!)
    }

    @Bean
    open fun tradesInfoService(limitOrderDatabaseAccessor: Optional<LimitOrderDatabaseAccessor>,
                               tradeInfoQueue: Optional<BlockingQueue<TradeInfo>>): TradesInfoService? {
        if (config.me.disableMarketHistory == true) {
            return null
        }
        return TradesInfoService(limitOrderDatabaseAccessor.get(), tradeInfoQueue.get())
    }

    @Bean
    open fun tradeInfoQueue(): BlockingQueue<TradeInfo>? {
        if (config.me.disableMarketHistory == true) {
            return null
        }
        return LinkedBlockingQueue<TradeInfo>()
    }

    @Bean
    open fun marketStateCache(historyTicksDatabaseAccessor: Optional<HistoryTicksDatabaseAccessor>,
                              @Value("\${application.tick.frequency:#{null}}")
                              frequency: Long?): MarketStateCache? {
        if (config.me.disableMarketHistory == true) {
            return null
        }
        return MarketStateCache(historyTicksDatabaseAccessor.get(), frequency!!)
    }
}