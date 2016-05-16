package com.lykke.matching.engine.database.azure

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlobContainer
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.DynamicTableEntity
import com.microsoft.azure.storage.table.TableBatchOperation
import com.microsoft.azure.storage.table.TableEntity

fun getOrCreateTable(connectionString: String, tableName: String): CloudTable {
    val storageAccount = CloudStorageAccount.parse(connectionString)
    val tableClient = storageAccount.createCloudTableClient()

    val cloudTable = tableClient.getTableReference(tableName)
    cloudTable.createIfNotExists()
    return cloudTable
}

fun getOrCreateBlob(connectionString: String, blobName: String): CloudBlobContainer {
    val storageAccount = CloudStorageAccount.parse(connectionString)
    val blobClient = storageAccount.createCloudBlobClient()

    val blob = blobClient.getContainerReference(blobName)
    blob.createIfNotExists()
    return blob
}

val AZURE_BATCH_SIZE = 100

fun batchInsertOrMerge(table: CloudTable, elements: List<TableEntity>) {
    if (elements.size == 0) {
        return
    }

    var batchOperation = TableBatchOperation()
    if (elements.size <= AZURE_BATCH_SIZE) {
        elements.forEach { element ->
            batchOperation.insertOrMerge(element)
        }
        table.execute(batchOperation)
    } else {
        elements.forEachIndexed { index, element ->
            batchOperation.insertOrMerge(element)
            if ((index + 1) % AZURE_BATCH_SIZE == 0) {
                table.execute(batchOperation)
                batchOperation = TableBatchOperation()
            }
        }
        if (batchOperation.size > 0) {
            table.execute(batchOperation)
        }
    }
}
fun batchDelete(table: CloudTable, elements: List<TableEntity>) {
    if (elements.size == 0) {
        return
    }

    var batchOperation = TableBatchOperation()
    if (elements.size <= AZURE_BATCH_SIZE) {
        elements.forEach { element ->
            val entity = DynamicTableEntity(element.partitionKey, element.rowKey)
            entity.etag = "*"
            batchOperation.delete(entity)
        }
        table.execute(batchOperation)
    } else {
        elements.forEachIndexed { index, element ->
            val entity = DynamicTableEntity(element.partitionKey, element.rowKey)
            entity.etag = "*"
            batchOperation.delete(entity)
            if ((index + 1) % AZURE_BATCH_SIZE == 0) {
                table.execute(batchOperation)
                batchOperation = TableBatchOperation()
            }
        }
        if (batchOperation.size > 0) {
            table.execute(batchOperation)
        }
    }
}