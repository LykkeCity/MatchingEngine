package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.azure.AzureHourCandle
import com.lykke.matching.engine.daos.azure.AzureHourCandle.MICRO
import com.lykke.matching.engine.database.HoursCandlesDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableQuery
import java.math.BigDecimal
import java.util.*
import java.util.stream.Collectors

class AzureHoursCandlesDatabaseAccessor(connectionString: String,
                                        hourCandlesTable: String) : HoursCandlesDatabaseAccessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureHoursCandlesDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val hourCandlesTable: CloudTable = getOrCreateTable(connectionString, hourCandlesTable)

    override fun getHoursCandles(): MutableList<HourCandle> {
        val result = ArrayList<HourCandle>()

        try {
            val partitionQuery = TableQuery.from(AzureHourCandle::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, MICRO))

            hourCandlesTable.execute(partitionQuery)
                    .mapTo(result) {
                        HourCandle(it.asset, it.pricesList.stream().map(BigDecimal::valueOf).collect(Collectors.toCollection(::LinkedList)))
                    }
        } catch (e: Exception) {
            LOGGER.error("Unable to load hour candles", e)
            METRICS_LOGGER.logError("Unable to load hour candles", e)
        }

        return result
    }

    override fun writeHourCandles(candles: List<HourCandle>) {
        try {
            batchInsertOrMerge(hourCandlesTable, candles.map {
                AzureHourCandle(it.asset, it.prices.stream().map(BigDecimal::toDouble).collect(Collectors.toCollection(::LinkedList)))
            })
        } catch (e: Exception) {
            LOGGER.error("Unable to save hour candles, size: ${candles.size}", e)
            METRICS_LOGGER.logError("Unable to save hour candles, size: ${candles.size}", e)
        }
    }

}