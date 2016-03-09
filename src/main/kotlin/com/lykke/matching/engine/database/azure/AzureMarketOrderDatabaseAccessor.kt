package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
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
    }

    val marketOrdersTable: CloudTable
    val tradesTable: CloudTable

    val DATE_FORMAT = {
        val format = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        format.timeZone = TimeZone.getTimeZone("UTC")
        format
    }.invoke()

    constructor(marketConfig: String?, tradesConfig: String?) {
        this.marketOrdersTable = getOrCreateTable(marketConfig!!, "MarketOrders")
        this.tradesTable = getOrCreateTable(tradesConfig!!, "Trades")
    }

    override fun addMarketOrder(order: MarketOrder) {
        try {
            marketOrdersTable.execute(TableOperation.insertOrMerge(order))
        } catch(e: Exception) {
            LOGGER.error("Unable to add market order: ${order.getId()}", e)
        }
    }

    override fun addMarketOrderWithGeneratedRowId(order: MarketOrder) {
        var counter = 0
        try {
            while (true) {
                try {
                    order.partitionKey = order.clientId
                    order.rowKey = "%s.%03d".format(DATE_FORMAT.format(order.matchedAt), counter)
                    marketOrdersTable.execute(TableOperation.insert(order))
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
            LOGGER.error("Unable to add market done order ${order.getId()}", e)
        }
    }

    override fun updateMarketOrder(order: MarketOrder) {
        try {
            marketOrdersTable.execute(TableOperation.merge(order))
        } catch(e: Exception) {
            LOGGER.error("Unable to update market order: ${order.getId()}", e)
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
        }
    }
}