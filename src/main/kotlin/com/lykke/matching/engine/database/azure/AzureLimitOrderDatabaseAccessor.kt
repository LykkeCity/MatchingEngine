package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.azure.AzureBestPrice
import com.lykke.matching.engine.daos.azure.AzureCandle
import com.lykke.matching.engine.daos.azure.AzureHourCandle
import com.lykke.matching.engine.daos.azure.AzureHourCandle.MICRO
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import java.math.BigDecimal
import java.util.*
import java.util.stream.Collectors

class AzureLimitOrderDatabaseAccessor constructor (connectionString: String,
                                                   bestPricesTable: String,
                                                   candlesTable: String,
                                                   hourCandlesTable: String) : LimitOrderDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(LimitOrderDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val bestPricesTable: CloudTable = getOrCreateTable(connectionString, bestPricesTable)
    private val candlesTable: CloudTable = getOrCreateTable(connectionString, candlesTable)
    private val hourCandlesTable: CloudTable = getOrCreateTable(connectionString, hourCandlesTable)

    override fun updateBestPrices(prices: List<BestPrice>) {
        try {
            batchInsertOrMerge(bestPricesTable, prices.map { AzureBestPrice(it.asset, it.ask.toDouble(), it.bid.toDouble()) })
        } catch(e: Exception) {
            LOGGER.error("Unable to update best prices, size: ${prices.size}", e)
            METRICS_LOGGER.logError( "Unable to update best prices, size: ${prices.size}", e)
        }
    }

    override fun writeCandle(candle: Candle) {
        try {
            candlesTable.execute(TableOperation.insertOrMerge(AzureCandle(candle.asset, candle.time, candle.data)))
        } catch(e: Exception) {
            LOGGER.error("Unable to add candle ${candle.asset} ${candle.time}", e)
            METRICS_LOGGER.logError( "Unable to add candle ${candle.asset} ${candle.time}", e)
        }
    }

    override fun getHoursCandles(): MutableList<HourCandle> {
        val result = ArrayList<HourCandle>()

        try {
            val partitionQuery = TableQuery.from(AzureHourCandle::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, MICRO))

            hourCandlesTable.execute(partitionQuery)
                    .mapTo(result) {
                        HourCandle(it.asset, it.pricesList.stream().map(BigDecimal::valueOf).collect(Collectors.toCollection(::LinkedList)))
                    }
        } catch(e: Exception) {
            LOGGER.error("Unable to load hour candles", e)
            METRICS_LOGGER.logError( "Unable to load hour candles", e)
        }

        return result
    }

    override fun writeHourCandles(candles: List<HourCandle>) {
        try {
            batchInsertOrMerge(hourCandlesTable, candles.map {
                AzureHourCandle(it.asset, it.prices.stream().map(BigDecimal::toDouble).collect(Collectors.toCollection(::LinkedList)))
            })
        } catch(e: Exception) {
            LOGGER.error("Unable to save hour candles, size: ${candles.size}", e)
            METRICS_LOGGER.logError( "Unable to save hour candles, size: ${candles.size}", e)
        }
    }
}