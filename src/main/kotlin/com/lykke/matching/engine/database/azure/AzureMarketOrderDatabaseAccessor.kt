package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.azure.AzureLkkTrade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.table.CloudTable
import org.apache.log4j.Logger

class AzureMarketOrderDatabaseAccessor(tradesConfig: String) : MarketOrderDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureMarketOrderDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val lkkTradesTable: CloudTable = getOrCreateTable(tradesConfig, "LkkTrades")

    override fun addLkkTrades(trades: List<LkkTrade>) {
        try {
            batchInsertOrMerge(lkkTradesTable, trades.map { AzureLkkTrade(it.assetPair, it.clientId, it.price, it.volume, it.date) })
        } catch(e: Exception) {
            LOGGER.error("Unable to add lkk trades, size: ${trades.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add lkk trades, size: ${trades.size}", e)
        }
    }
}