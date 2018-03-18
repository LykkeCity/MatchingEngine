package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.azure.AzureLkkTrade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import org.springframework.beans.factory.annotation.Value

class AzureMarketOrderDatabaseAccessor(tradesConfig: String) : MarketOrderDatabaseAccessor {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(AzureMarketOrderDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Value("\${azure.market.order.table}")
    private lateinit var tableName: String


    private val lkkTradesTable: CloudTable = getOrCreateTable(tradesConfig, tableName)

    override fun addLkkTrades(trades: List<LkkTrade>) {
        try {
            batchInsertOrMerge(lkkTradesTable, trades.map { AzureLkkTrade(it.assetPair, it.clientId, it.price, it.volume, it.date) })
        } catch(e: Exception) {
            LOGGER.error("Unable to add lkk trades, size: ${trades.size}", e)
            METRICS_LOGGER.logError( "Unable to add lkk trades, size: ${trades.size}", e)
        }
    }
}