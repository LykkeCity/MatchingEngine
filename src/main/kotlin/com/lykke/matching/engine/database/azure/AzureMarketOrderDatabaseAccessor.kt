package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import org.apache.log4j.Logger
import java.util.Properties

class AzureMarketOrderDatabaseAccessor: MarketOrderDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureMarketOrderDatabaseAccessor::class.java.name)
    }

    val marketOrdersTable: CloudTable
    val tradesTable: CloudTable

    constructor(config: Properties) {
        val storageConnectionString =
                "DefaultEndpointsProtocol=${config.getProperty("azure.default.endpoints.protocol")};" +
                        "AccountName=${config.getProperty("azure.account.name")};" +
                        "AccountKey=${config.getProperty("azure.account.key")}"

        this.marketOrdersTable = getOrCreateTable(storageConnectionString, "MarketOrders")
        this.tradesTable = getOrCreateTable(storageConnectionString, "Trades")
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
        try {
            batchInsertOrMerge(tradesTable, trades)
        } catch(e: Exception) {
            LOGGER.error("Unable to add trades, size: ${trades.size}", e)
        }
    }
}