package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.azure.AzureWalletSwapOperation
import com.lykke.matching.engine.daos.azure.AzureWalletTransferOperation
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.TableOperation

class AzureCashOperationsDatabaseAccessor(connectionString: String,
                                          tableName: String) : CashOperationsDatabaseAccessor {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureCashOperationsDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val transferOperationsTable = getOrCreateTable(connectionString, tableName)

    override fun insertTransferOperation(operation: TransferOperation) {
        try {
            transferOperationsTable.execute(TableOperation.insertOrMerge(AzureWalletTransferOperation(operation.matchingEngineOperationId, operation.externalId,
                    operation.fromClientId, operation.toClientId, operation.asset!!.assetId, operation.dateTime, operation.volume.toDouble())))
        } catch (e: Exception) {
            LOGGER.error("Unable to insert operation: ${operation.matchingEngineOperationId}, external id: ${operation.externalId}", e)
            METRICS_LOGGER.logError("Unable to insert operation: ${operation.matchingEngineOperationId}, external id: ${operation.externalId}", e)
        }
    }

    override fun insertSwapOperation(operation: SwapOperation) {
        try {
            transferOperationsTable
                    .execute(TableOperation.insertOrMerge(
                            AzureWalletSwapOperation(operation.id, operation.externalId,
                                    operation.clientId1, operation.asset1, operation.volume1.toDouble(),
                                    operation.clientId2, operation.asset2, operation.volume2.toDouble(),
                                    operation.dateTime)))
        } catch (e: Exception) {
            LOGGER.error("Unable to insert swap operation: ${operation.id}, external id: ${operation.externalId}", e)
            METRICS_LOGGER.logError("Unable to insert swap operation: ${operation.id}, external id: ${operation.externalId}", e)
        }
    }
}