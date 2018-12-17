package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.azure.AzureBestPrice
import com.lykke.matching.engine.database.BestPriceDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable

class AzureBestPriceDatabaseAccessor(connectionString: String,
                                     bestPricesTable: String) : BestPriceDatabaseAccessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureBestPriceDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val bestPricesTable: CloudTable = getOrCreateTable(connectionString, bestPricesTable)

    override fun updateBestPrices(prices: List<BestPrice>) {
        try {
            batchInsertOrMerge(bestPricesTable, prices.map { AzureBestPrice(it.asset, it.ask.toDouble(), it.bid.toDouble()) })
        } catch (e: Exception) {
            LOGGER.error("Unable to update best prices, size: ${prices.size}", e)
            METRICS_LOGGER.logError("Unable to update best prices, size: ${prices.size}", e)
        }
    }
}