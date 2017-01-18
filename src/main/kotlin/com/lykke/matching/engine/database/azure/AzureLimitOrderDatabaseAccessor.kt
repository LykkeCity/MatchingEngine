package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.Candle
import com.lykke.matching.engine.daos.HourCandle
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.azure.AzureBestPrice
import com.lykke.matching.engine.daos.azure.AzureCandle
import com.lykke.matching.engine.daos.azure.AzureHourCandle
import com.lykke.matching.engine.daos.azure.AzureHourCandle.MICRO
import com.lykke.matching.engine.daos.azure.AzureLimitOrder
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import com.microsoft.azure.storage.table.TableServiceException
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Date
import java.util.TimeZone

class AzureLimitOrderDatabaseAccessor(activeOrdersConfig: String, historyOrdersConfig: String, liquidityConfig: String) : LimitOrderDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(LimitOrderDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()

        val ORDER_ID = "OrderId"
    }

    val limitOrdersTable: CloudTable
    val limitOrdersDoneTable: CloudTable
    val bestPricesTable: CloudTable
    val candlesTable: CloudTable
    val hourCandlesTable: CloudTable

    val DATE_FORMAT = {
        val format = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        format.timeZone = TimeZone.getTimeZone("UTC")
        format
    }.invoke()

    init {
        this.limitOrdersTable = getOrCreateTable(activeOrdersConfig, "LimitOrders")
        this.limitOrdersDoneTable = getOrCreateTable(historyOrdersConfig, "LimitOrdersDone")
        this.bestPricesTable = getOrCreateTable(liquidityConfig, "MarketProfile")
        this.candlesTable = getOrCreateTable(liquidityConfig, "FeedHistory")
        this.hourCandlesTable = getOrCreateTable(liquidityConfig, "FeedHoursHistory")
    }

    override fun loadLimitOrders(): List<LimitOrder> {
        val result = ArrayList<LimitOrder>()
        try {
            val partitionQuery = TableQuery.from(AzureLimitOrder::class.java)

            limitOrdersTable.execute(partitionQuery).mapTo(result) { it.toLimitOrder() }
        } catch(e: Exception) {
            LOGGER.error("Unable to load limit orders", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load limit orders", e)
        }
        LOGGER.info("Loaded ${result.size} active limit orders")
        return result
    }

    override fun addLimitOrder(order: LimitOrder) {
        try {
            limitOrdersTable.execute(TableOperation.insertOrMerge(AzureLimitOrder(order.assetPairId, order)))
        } catch(e: Exception) {
            LOGGER.error("Unable to add limit order: ${order.id}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add limit order: ${order.id}", e)
        }
    }

    override fun addLimitOrders(orders: List<LimitOrder>) {
        try {
            batchInsertOrMerge(limitOrdersTable, orders.map({ AzureLimitOrder(it.assetPairId, it) }))
        } catch(e: Exception) {
            LOGGER.error("Unable to add limit orders, size: ${orders.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add limit order, size: ${orders.size}", e)
        }
    }

    override fun updateLimitOrder(order: LimitOrder) {
        try {
            limitOrdersTable.execute(TableOperation.insertOrReplace(AzureLimitOrder(order.assetPairId, order)))
        } catch(e: Exception) {
            LOGGER.error("Unable to update limit order: ${order.id}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to update limit order: ${order.id}", e)
        }
    }

    override fun deleteLimitOrders(orders: List<LimitOrder>) {
        try {
            batchDelete(limitOrdersTable, orders.map( { AzureLimitOrder(it.assetPairId, it) } ))
        } catch(e: Exception) {
            LOGGER.error("Unable to delete limit orders, size: ${orders.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to delete limit orders, size: ${orders.size}", e)
        }
    }

    override fun addLimitOrderDone(order: LimitOrder) {
        try {
            if (order.remainingVolume != order.volume) {
                limitOrdersDoneTable.execute(TableOperation.insertOrMerge(AzureLimitOrder(ORDER_ID, order)))
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to add limit done order ${order.id}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add limit done order ${order.id}", e)
        }
    }

    override fun addLimitOrdersDone(orders: List<LimitOrder>) {
        try {
            batchInsertOrMerge(limitOrdersDoneTable, orders.filter { it.remainingVolume != it.volume }.map( { AzureLimitOrder(ORDER_ID, it) } ))
        } catch(e: Exception) {
            LOGGER.error("Unable to add limit done orders, size: ${orders.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add limit done orders, size: ${orders.size}", e)
        }
    }

    override fun addLimitOrderDoneWithGeneratedRowId(order: LimitOrder) {
        var counter = 0
        try {
            val azureOrder = AzureLimitOrder(order.clientId, order)
            while (true) {
                try {
                    azureOrder.rowKey = String.format("%s.%03d", DATE_FORMAT.format(order.lastMatchTime ?: Date()), counter)
                    limitOrdersDoneTable.execute(TableOperation.insert(azureOrder))
                    return
                } catch(e: TableServiceException) {
                    if (e.httpStatusCode == 409 && counter < 999) {
                        counter++
                    } else {
                        throw e
                    }
                }
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to add limit done order ${order.id}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add limit done order ${order.id}", e)
        }
    }

    override fun updateBestPrices(prices: List<BestPrice>) {
        try {
            batchInsertOrMerge(bestPricesTable, prices.map { AzureBestPrice(it.asset, it.ask, it.bid) })
        } catch(e: Exception) {
            LOGGER.error("Unable to update best prices, size: ${prices.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to update best prices, size: ${prices.size}", e)
        }
    }

    override fun writeCandle(candle: Candle) {
        try {
            candlesTable.execute(TableOperation.insertOrMerge(AzureCandle(candle.asset, candle.time, candle.data)))
        } catch(e: Exception) {
            LOGGER.error("Unable to add candle ${candle.asset} ${candle.time}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add candle ${candle.asset} ${candle.time}", e)
        }
    }

    override fun getHoursCandles(): MutableList<HourCandle> {
        val result = ArrayList<HourCandle>()

        try {
            val partitionQuery = TableQuery.from(AzureHourCandle::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, MICRO))

            hourCandlesTable.execute(partitionQuery).mapTo(result) { HourCandle(it.asset, it.pricesList) }
        } catch(e: Exception) {
            LOGGER.error("Unable to load hour candles", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load hour candles", e)
        }

        return result
    }

    override fun writeHourCandles(candles: List<HourCandle>) {
        try {
            batchInsertOrMerge(hourCandlesTable, candles.map { AzureHourCandle(it.asset, it.prices) })
        } catch(e: Exception) {
            LOGGER.error("Unable to save hour candles, size: ${candles.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to save hour candles, size: ${candles.size}", e)
        }
    }
}