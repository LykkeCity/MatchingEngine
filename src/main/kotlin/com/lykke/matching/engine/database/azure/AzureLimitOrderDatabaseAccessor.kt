package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.LimitOrderDatabaseAccessor
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableBatchOperation
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import java.util.ArrayList
import java.util.Properties

class AzureLimitOrderDatabaseAccessor: LimitOrderDatabaseAccessor {

    val limitOrdersTable: CloudTable
    val limitOrdersDoneTable: CloudTable

    constructor(config: Properties) {
        val storageConnectionString =
                "DefaultEndpointsProtocol=${config.getProperty("azure.default.endpoints.protocol")};" +
                        "AccountName=${config.getProperty("azure.account.name")};" +
                        "AccountKey=${config.getProperty("azure.account.key")}"
        val storageAccount = CloudStorageAccount.parse(storageConnectionString)
        val tableClient = storageAccount.createCloudTableClient()

        this.limitOrdersTable = tableClient.getTableReference("LimitOrders")
        this.limitOrdersDoneTable = tableClient.getTableReference("LimitOrdersDone")
    }

    override fun loadLimitOrders(): List<LimitOrder> {
        val result = ArrayList<LimitOrder>()
        val partitionQuery = TableQuery.from(LimitOrder::class.java)

        for (order in limitOrdersTable.execute(partitionQuery)){
            result.add(order)
        }

        return result
    }

    override fun addLimitOrder(order: LimitOrder) {
        limitOrdersTable.execute(TableOperation.insertOrMerge(order))
    }

    override fun updateLimitOrder(order: LimitOrder) {
        limitOrdersTable.execute(TableOperation.merge(order))
    }

    override fun deleteLimitOrders(order: List<LimitOrder>) {
        val batchOperation = TableBatchOperation()
        order.forEach { order ->
            batchOperation.delete(order)
        }
        limitOrdersTable.execute(batchOperation)
    }

    override fun saveLimitOrdersDone(order: List<LimitOrder>) {
        val batchOperation = TableBatchOperation()
        order.forEach { order ->
            batchOperation.insertOrMerge(order)
        }
        limitOrdersTable.execute(batchOperation)
    }
}