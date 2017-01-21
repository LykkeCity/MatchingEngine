package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.azure.AzureAssetPair
import com.lykke.matching.engine.daos.azure.AzureExternalCashOperation
import com.lykke.matching.engine.daos.azure.AzureWalletOperation
import com.lykke.matching.engine.daos.azure.AzureWalletTransferOperation
import com.lykke.matching.engine.daos.azure.wallet.AzureWallet
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons.EQUAL
import org.apache.log4j.Logger
import java.util.HashMap


class AzureWalletDatabaseAccessor(balancesConfig: String, dictsConfig: String) : WalletDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureWalletDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val accountTable: CloudTable
    val operationsTable: CloudTable
    val transferOperationsTable: CloudTable
    val externalOperationsTable: CloudTable
    val assetsTable: CloudTable

    private val ASSET_PAIR = "AssetPair"
    private val PARTITION_KEY = "PartitionKey"
    private val CLIENT_BALANCE = "ClientBalance"

    override fun loadBalances(): HashMap<String, MutableMap<String, Double>> {
        val result = HashMap<String, MutableMap<String, Double>>()
        var balancesCount = 0
        try {
            val partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, CLIENT_BALANCE)
            val partitionQuery = TableQuery.from(AzureWallet::class.java).where(partitionFilter)

            accountTable.execute(partitionQuery).forEach { wallet ->
                val map = result.getOrPut(wallet.rowKey) { HashMap<String, Double>() }
                wallet.balancesList.forEach { balance ->
                    if (balance.balance != null) {
                        map.put(balance.asset, balance.balance)
                        balancesCount++
                    }
                }
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load balances", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load balances", e)
        }
        LOGGER.info("Loaded $balancesCount balances for ${result.size} clients")
        return result
    }

    override fun loadWallets(): HashMap<String, Wallet> {
        val result = HashMap<String, Wallet>()
        try {
            val partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, CLIENT_BALANCE)
            val partitionQuery = TableQuery.from(AzureWallet::class.java).where(partitionFilter)

            accountTable.execute(partitionQuery).forEach { wallet ->
                result.put(wallet.rowKey, Wallet(wallet.clientId, wallet.balancesList.map { AssetBalance(it.asset, it.balance)}))
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load accounts", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load accounts", e)
        }

        return result
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        try {
            batchInsertOrMerge(accountTable, wallets.map { AzureWallet(it.clientId, it.balances.map { it.value })})
        } catch(e: Exception) {
            LOGGER.error("Unable to update accounts, size: ${wallets.size}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to update accounts, size: ${wallets.size}", e)
        }
    }

    override fun insertExternalCashOperation(operation: ExternalCashOperation) {
        try {
            externalOperationsTable.execute(TableOperation.insertOrMerge(AzureExternalCashOperation(operation.clientId, operation.externalId, operation.cashOperationId)))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert external operation: ${operation.clientId}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to insert external operation: ${operation.clientId}", e)
        }
    }

    override fun loadExternalCashOperation(clientId: String, operationId: String): ExternalCashOperation? {
        try {
            val retrieveOperation = TableOperation.retrieve(clientId, operationId, AzureExternalCashOperation::class.java)
            val operation = externalOperationsTable.execute(retrieveOperation).getResultAsType<AzureExternalCashOperation>()
            if (operation != null) {
                return ExternalCashOperation(operation.partitionKey, operation.rowKey, operation.cashOperationId)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to check if operation processed: $clientId, $operationId", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to check if operation processed: $clientId, $operationId", e)
        }
        return null
    }

    override fun insertOperation(operation: WalletOperation) {
        try {
            operationsTable.execute(TableOperation.insertOrMerge(AzureWalletOperation(operation.clientId, operation.uid, operation.assetId, operation.dateTime, operation.amount, operation.transactionId)))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert operation: ${operation.uid}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to insert operation: ${operation.uid}", e)
        }
    }

    override fun insertTransferOperation (operation: TransferOperation) {
        try {
            transferOperationsTable.execute(TableOperation.insertOrMerge(AzureWalletTransferOperation(operation.fromClientId, operation.toClientId, operation.uid, operation.assetId, operation.dateTime, operation.amount)))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert operation: ${operation.uid}", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to insert operation: ${operation.uid}", e)
        }
    }

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        val result = HashMap<String, AssetPair>()

        try {
            val partitionQuery = TableQuery.from(AzureAssetPair::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", EQUAL, ASSET_PAIR))

            for (asset in assetsTable.execute(partitionQuery)) {
                result[asset.assetPairId] = AssetPair(asset.assetPairId, asset.baseAssetId, asset.quotingAssetId, asset.accuracy, asset.invertedAccuracy)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset pairs", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load asset pairs", e)
        }

        return result
    }

    override fun loadAssetPair(assetId: String): AssetPair? {
        try {
            val retrieveAssetPair = TableOperation.retrieve(ASSET_PAIR, assetId, AzureAssetPair::class.java)
            val assetPair = assetsTable.execute(retrieveAssetPair).getResultAsType<AzureAssetPair>()
            if (assetPair != null) {
                return AssetPair(assetPair.assetPairId, assetPair.baseAssetId, assetPair.quotingAssetId, assetPair.accuracy, assetPair.invertedAccuracy)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset: $assetId", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load asset: $assetId", e)
        }
        return null
    }

    init {
        this.accountTable = getOrCreateTable(balancesConfig, "Accounts")
        this.operationsTable = getOrCreateTable(balancesConfig, "OperationsCash")
        this.transferOperationsTable = getOrCreateTable(balancesConfig, "TransferOperationsCash")
        this.externalOperationsTable = getOrCreateTable(balancesConfig, "ExternalOperationsCash")
        this.assetsTable = getOrCreateTable(dictsConfig, "Dictionaries")
    }
}