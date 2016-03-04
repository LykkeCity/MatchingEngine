package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import org.apache.log4j.Logger
import java.util.HashMap
import java.util.LinkedList

class AzureMarketOrderDatabaseAccessor: MarketOrderDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureMarketOrderDatabaseAccessor::class.java.name)
    }

    val marketOrdersTable: CloudTable
    val tradesTable: CloudTable

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