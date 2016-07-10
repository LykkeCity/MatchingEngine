package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MatchingData
import com.lykke.matching.engine.daos.OrderTradesLink
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableServiceException
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import java.util.HashMap
import java.util.LinkedList
import java.util.TimeZone

class AzureMarketOrderDatabaseAccessor: MarketOrderDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureMarketOrderDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val marketOrdersTable: CloudTable
    val matchingDataTable: CloudTable
    val orderTradesLinksTable: CloudTable
    val tradesTable: CloudTable

    val DATE_FORMAT = {
        val format = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        format.timeZone = TimeZone.getTimeZone("UTC")
        format
    }.invoke()

    constructor(marketConfig: String, tradesConfig: String) {
        this.marketOrdersTable = getOrCreateTable(marketConfig, "MarketOrders")
        this.matchingDataTable = getOrCreateTable(marketConfig, "MatchingData")
        this.orderTradesLinksTable = getOrCreateTable(marketConfig, "OrderTradesLinks")
        this.tradesTable = getOrCreateTable(tradesConfig, "Trades")
    }

    override fun addMarketOrder(order: MarketOrder) {
        try {
            marketOrdersTable.execute(TableOperation.insertOrMerge(order))
        } catch(e: Exception) {
            LOGGER.error("Unable to add market order: ${order.getId()}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add market order: ${order.getId()}", e)
        }
    }

    override fun addMarketOrderWithGeneratedRowId(order: MarketOrder) {
        var counter = 0
        val dateString = DATE_FORMAT.format(order.matchedAt)
        try {
            while (true) {
                try {
                    val partitionKey = order.partitionKey
                    val orderId = order.id
                    order.partitionKey = order.clientId
                    order.rowKey = String.format("%s.%03d", dateString, counter)
                    marketOrdersTable.execute(TableOperation.insert(order))

                    order.partitionKey = partitionKey
                    order.rowKey = orderId
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
            LOGGER.error("Unable to add market order ${order.getId()}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add market order ${order.getId()}", e)
        }
    }

    override fun updateMarketOrder(order: MarketOrder) {
        try {
            marketOrdersTable.execute(TableOperation.merge(order))
        } catch(e: Exception) {
            LOGGER.error("Unable to update market order: ${order.getId()}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to update market order: ${order.getId()}", e)
        }
    }

    override fun addTrades(trades: List<Trade>) {
        val tradesByPartition = HashMap<String, MutableList<Trade>>()
        trades.forEach { trade ->
            val client = tradesByPartition.getOrPut(trade.getClientId()) { LinkedList<Trade>() }
            client.add(trade)
        }

        try {
            tradesByPartition.values.forEach {
                batchInsertOrMerge(tradesTable, it)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to add trades, size: ${trades.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add trades, size: ${trades.size}", e)
        }
    }

    override fun addMatchingData(data: List<MatchingData>) {
        val dataByPartition = HashMap<String, MutableList<MatchingData>>()
        data.forEach { curData ->
            val partition = dataByPartition.getOrPut(curData.partitionKey) { LinkedList<MatchingData>() }
            partition.add(curData)
        }

        try {
            dataByPartition.values.forEach {
                batchInsertOrMerge(matchingDataTable, it)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to add matching data, size: ${data.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add matching data, size: ${data.size}", e)
        }
    }

    override fun addOrderTradesLinks(links: List<OrderTradesLink>) {
        val linksByPartition = HashMap<String, MutableList<OrderTradesLink>>()
        links.forEach { curLink ->
            val partition = linksByPartition.getOrPut(curLink.partitionKey) { LinkedList<OrderTradesLink>() }
            partition.add(curLink)
        }

        try {
            linksByPartition.values.forEach {
                batchInsertOrMerge(orderTradesLinksTable, it)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to add order trades links, size: ${links.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add order trades links, size: ${links.size}", e)
        }
    }
}