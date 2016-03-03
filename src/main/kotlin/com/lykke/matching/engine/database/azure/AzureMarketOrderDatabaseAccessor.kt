package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.Trade
import com.lykke.matching.engine.database.MarketOrderDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import java.util.Properties

class AzureMarketOrderDatabaseAccessor: MarketOrderDatabaseAccessor {

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
        marketOrdersTable.execute(TableOperation.insertOrMerge(order))
    }

    override fun updateMarketOrder(order: MarketOrder) {
        marketOrdersTable.execute(TableOperation.merge(order))
    }

    override fun addTrades(trades: List<Trade>) {
        batchInsertOrMerge(tradesTable, trades)
    }
}