package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import org.apache.log4j.Logger
import java.util.ArrayList

class AzureLimitOrderDatabaseAccessor: LimitOrderDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureLimitOrderDatabaseAccessor::class.java.name)
    }

    val limitOrdersTable: CloudTable
    val limitOrdersDoneTable: CloudTable

    constructor(activeOrdersConfig: String?, historyOrdersConfig: String?) {
        this.limitOrdersTable = getOrCreateTable(activeOrdersConfig!!, "LimitOrders")
        this.limitOrdersDoneTable = getOrCreateTable(historyOrdersConfig!!, "LimitOrdersDone")
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

    override fun addLimitOrdersDone(orders: List<LimitOrder>) {
        try {
            batchInsertOrMerge(limitOrdersDoneTable, orders)
        } catch(e: Exception) {
            LOGGER.error("Unable to add limit done orders, size: ${orders.size}", e)
        }
    }
}