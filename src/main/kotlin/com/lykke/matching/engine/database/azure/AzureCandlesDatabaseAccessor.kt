package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.azure.AzureCandle
import com.lykke.matching.engine.database.CandlesDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation

class AzureCandlesDatabaseAccessor(connectionString: String,
                                   candlesTable: String) : CandlesDatabaseAccessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureCandlesDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val candlesTable: CloudTable = getOrCreateTable(connectionString, candlesTable)

    override fun writeCandle(candle: Candle) {
        try {
            candlesTable.execute(TableOperation.insertOrMerge(AzureCandle(candle.asset, candle.time, candle.data)))
        } catch (e: Exception) {
            LOGGER.error("Unable to add candle ${candle.asset} ${candle.time}", e)
            METRICS_LOGGER.logError("Unable to add candle ${candle.asset} ${candle.time}", e)
        }
    }
}