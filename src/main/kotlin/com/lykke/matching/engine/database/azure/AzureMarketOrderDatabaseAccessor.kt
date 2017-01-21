package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.MatchingData
import com.lykke.matching.engine.daos.OrderTradesLink
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.daos.azure.AzureLkkTrade
import com.lykke.matching.engine.daos.azure.AzureMarketOrder
import com.lykke.matching.engine.daos.azure.AzureMatchingData
import com.lykke.matching.engine.daos.azure.AzureOrderTradesLink
import com.lykke.matching.engine.daos.azure.AzureTrade
import com.lykke.matching.engine.daos.azure.AzureTrade.DATE_TIME
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

class AzureMarketOrderDatabaseAccessor(marketConfig: String, tradesConfig: String) : MarketOrderDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureMarketOrderDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val marketOrdersTable: CloudTable
    val matchingDataTable: CloudTable
    val orderTradesLinksTable: CloudTable
    val tradesTable: CloudTable
    val lkkTradesTable: CloudTable

    val DATE_FORMAT = {
        val format = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        format.timeZone = TimeZone.getTimeZone("UTC")
        format
    }.invoke()

    init {
        this.marketOrdersTable = getOrCreateTable(marketConfig, "MarketOrders")
        this.matchingDataTable = getOrCreateTable(marketConfig, "MatchingData")
        this.orderTradesLinksTable = getOrCreateTable(marketConfig, "OrderTradesLinks")
        this.tradesTable = getOrCreateTable(tradesConfig, "Trades")
        this.lkkTradesTable = getOrCreateTable(tradesConfig, "LkkTrades")
    }

    override fun addMarketOrder(order: MarketOrder) {
        try {
            marketOrdersTable.execute(TableOperation.insertOrMerge(AzureMarketOrder(order)))
        } catch(e: Exception) {
            LOGGER.error("Unable to add market order: ${order.id}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add market order: ${order.id}", e)
        }
    }

    override fun addMarketOrderWithGeneratedRowId(order: MarketOrder) {
        var counter = 0
        val dateString = DATE_FORMAT.format(order.matchedAt)
        try {
            val azureOrder = AzureMarketOrder(order)
            while (true) {
                try {
                    azureOrder.partitionKey = azureOrder.clientId
                    azureOrder.rowKey = String.format("%s.%03d", dateString, counter)
                    marketOrdersTable.execute(TableOperation.insertOrMerge(azureOrder))
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
            LOGGER.error("Unable to add market order ${order.id}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add market order ${order.id}", e)
        }
    }

    override fun updateMarketOrder(order: MarketOrder) {
        try {
            marketOrdersTable.execute(TableOperation.insertOrMerge(AzureMarketOrder(order)))
        } catch(e: Exception) {
            LOGGER.error("Unable to update market order: ${order.id}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to update market order: ${order.id}", e)
        }
    }

    override fun addTrades(trades: List<Trade>) {
        val tradesByPartition = HashMap<String, MutableList<AzureTrade>>()
        trades.forEach { trade ->
            val client = tradesByPartition.getOrPut(trade.clientId) { LinkedList<AzureTrade>() }
            val azureTrade = AzureTrade(trade.clientId, trade.uid, trade.clientId, trade.multisig, trade.assetId, trade.dateTime, trade.limitOrderId, trade.marketOrderId, trade.volume, trade.price, trade.addressFrom, trade.addressTo)
            client.add(azureTrade)
            val date = tradesByPartition.getOrPut(DATE_TIME) { LinkedList<AzureTrade>() }
            date.add(azureTrade.cloneWithGeneratedId())
            val multisig = tradesByPartition.getOrPut(trade.multisig) { LinkedList<AzureTrade>() }
            multisig.add(azureTrade.cloneWithMultisig())
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

    override fun addLkkTrades(trades: List<LkkTrade>) {
        try {
            batchInsertOrMerge(lkkTradesTable, trades.map { AzureLkkTrade(it.assetPair, it.price, it.volume, it.date) })
        } catch(e: Exception) {
            LOGGER.error("Unable to add lkk trades, size: ${trades.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to add lkk trades, size: ${trades.size}", e)
        }
    }

    override fun addMatchingData(data: List<MatchingData>) {
        val dataByPartition = HashMap<String, MutableList<AzureMatchingData>>()
        data.forEach { curData ->
            val partition = dataByPartition.getOrPut(curData.masterOrderId) { LinkedList<AzureMatchingData>() }
            partition.add(AzureMatchingData(curData.masterOrderId, curData.matchedOrderId, curData.volume))
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
        val linksByPartition = HashMap<String, MutableList<AzureOrderTradesLink>>()
        links.forEach { curLink ->
            val partition = linksByPartition.getOrPut(curLink.orderId) { LinkedList<AzureOrderTradesLink>() }
            partition.add(AzureOrderTradesLink(curLink.orderId, curLink.tradeId))
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