package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.azure.AzureExternalCashOperation
import com.lykke.matching.engine.daos.azure.AzureWalletOperation
import com.lykke.matching.engine.daos.azure.AzureWalletSwapOperation
import com.lykke.matching.engine.daos.azure.AzureWalletTransferOperation
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.lykke.matching.engine.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.TableOperation

class AzureCashOperationsDatabaseAccessor(balancesConfig: String) : CashOperationsDatabaseAccessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureCashOperationsDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val operationsTable = getOrCreateTable(balancesConfig, "OperationsCash")
    private val transferOperationsTable = getOrCreateTable(balancesConfig, "SwapOperationsCash")
    private val externalOperationsTable = getOrCreateTable(balancesConfig, "ExternalOperationsCash")

    override fun insertExternalCashOperation(operation: ExternalCashOperation) {
        try {
            externalOperationsTable.execute(TableOperation.insertOrMerge(AzureExternalCashOperation(operation.clientId, operation.externalId, operation.cashOperationId)))
        } catch (e: Exception) {
            LOGGER.error("Unable to insert external operation: ${operation.clientId}", e)
            METRICS_LOGGER.logError("Unable to insert external operation: ${operation.clientId}", e)
        }
    }

    override fun loadExternalCashOperation(clientId: String, operationId: String): ExternalCashOperation? {
        try {
            val retrieveOperation = TableOperation.retrieve(clientId, operationId, AzureExternalCashOperation::class.java)
            val operation = externalOperationsTable.execute(retrieveOperation).getResultAsType<AzureExternalCashOperation>()
            if (operation != null) {
                return ExternalCashOperation(operation.partitionKey, operation.rowKey, operation.cashOperationId)
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to check if operation processed: $clientId, $operationId", e)
            METRICS_LOGGER.logError("Unable to check if operation processed: $clientId, $operationId", e)
        }
        return null
    }

    override fun insertOperation(operation: WalletOperation) {
        try {
            operationsTable.execute(TableOperation.insertOrMerge(AzureWalletOperation(operation.id, operation.externalId, operation.clientId, operation.assetId, operation.dateTime, operation.amount)))
        } catch (e: Exception) {
            LOGGER.error("Unable to insert operation: ${operation.id}, external id: ${operation.externalId}", e)
            METRICS_LOGGER.logError("Unable to insert operation: ${operation.id}, external id: ${operation.externalId}", e)
        }
    }

    override fun insertTransferOperation(operation: TransferOperation) {
        try {
            transferOperationsTable.execute(TableOperation.insertOrMerge(AzureWalletTransferOperation(operation.id, operation.externalId, operation.fromClientId, operation.toClientId, operation.asset, operation.dateTime, operation.volume)))
        } catch (e: Exception) {
            LOGGER.error("Unable to insert operation: ${operation.id}, external id: ${operation.externalId}", e)
            METRICS_LOGGER.logError("Unable to insert operation: ${operation.id}, external id: ${operation.externalId}", e)
        }
    }

    override fun insertSwapOperation(operation: SwapOperation) {
        try {
            transferOperationsTable.execute(TableOperation.insertOrMerge(AzureWalletSwapOperation(operation.id, operation.externalId, operation.clientId1, operation.asset1, operation.volume1, operation.clientId2, operation.asset2, operation.volume2, operation.dateTime)))
        } catch (e: Exception) {
            LOGGER.error("Unable to insert swap operation: ${operation.id}, external id: ${operation.externalId}", e)
            METRICS_LOGGER.logError("Unable to insert swap operation: ${operation.id}, external id: ${operation.externalId}", e)
        }
    }
}