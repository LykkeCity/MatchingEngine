package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.azure.AzureWalletSwapOperation
import com.lykke.matching.engine.daos.azure.AzureWalletTransferOperation
import com.lykke.matching.engine.database.CashOperationsDatabaseAccessor
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.TableOperation
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class AzureCashOperationsDatabaseAccessor : CashOperationsDatabaseAccessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureCashOperationsDatabaseAccessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Value("\${azure.cache.operation.table}")
    private lateinit var tableName: String

    @Autowired
    private lateinit var config: Config

    private val transferOperationsTable = getOrCreateTable(config.me.db.balancesInfoConnString, tableName)

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