package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import com.microsoft.azure.storage.table.TableServiceException
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.TimeZone

class AzureLimitOrderDatabaseAccessor: LimitOrderDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureLimitOrderDatabaseAccessor::class.java.name)
    }

    val limitOrdersTable: CloudTable
    val limitOrdersDoneTable: CloudTable
    val bestPricesTable: CloudTable

    val DATE_FORMAT = {
        val format = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        format.timeZone = TimeZone.getTimeZone("UTC")
        format
    }.invoke()

    constructor(activeOrdersConfig: String, historyOrdersConfig: String, liquidityConfig: String) {
        this.limitOrdersTable = getOrCreateTable(activeOrdersConfig, "LimitOrders")
        this.limitOrdersDoneTable = getOrCreateTable(historyOrdersConfig, "LimitOrdersDone")
        this.bestPricesTable = getOrCreateTable(liquidityConfig, "MarketProfile")
    }

    override fun loadLimitOrders(): List<LimitOrder> {
        val result = ArrayList<LimitOrder>()
        try {
            val partitionQuery = TableQuery.from(LimitOrder::class.java)

            for (order in limitOrdersTable.execute(partitionQuery)){
                result.add(order)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load limit orders", e)
        }
        return result
    }

    override fun addLimitOrder(order: LimitOrder) {
        try {
            limitOrdersTable.execute(TableOperation.insertOrMerge(order))
        } catch(e: Exception) {
            LOGGER.error("Unable to add limit order: ${order.getId()}", e)
        }
    }

    override fun updateLimitOrder(order: LimitOrder) {
        try {
            limitOrdersTable.execute(TableOperation.merge(order))
        } catch(e: Exception) {
            LOGGER.error("Unable to update limit order: ${order.getId()}", e)
        }
    }

    override fun deleteLimitOrders(orders: List<LimitOrder>) {
        try {
            batchDelete(limitOrdersTable, orders)
        } catch(e: Exception) {
            LOGGER.error("Unable to delete limit orders, size: ${orders.size}", e)
        }
    }

    override fun addLimitOrderDone(order: LimitOrder) {
        try {
            limitOrdersDoneTable.execute(TableOperation.insertOrMerge(order))
        } catch(e: Exception) {
            LOGGER.error("Unable to add limit done order ${order.getId()}", e)
        }
    }

    override fun addLimitOrderDoneWithGeneratedRowId(order: LimitOrder) {
        var counter = 0
        try {
            while (true) {
                try {
                    order.partitionKey = order.clientId
                    order.rowKey = "%s.%03d".format(DATE_FORMAT.format(order.lastMatchTime), counter)
                    limitOrdersDoneTable.execute(TableOperation.insert(order))
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
            LOGGER.error("Unable to add limit done order ${order.getId()}", e)
        }
    }

    override fun updateBestPrices(prices: List<BestPrice>) {
        try {
            batchInsertOrMerge(bestPricesTable, prices)
        } catch(e: Exception) {
            LOGGER.error("Unable to update best prices, size: ${prices.size}", e)
        }
    }
}