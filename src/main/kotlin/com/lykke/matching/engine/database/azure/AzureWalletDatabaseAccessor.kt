package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.ExternalCashOperation
import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons.EQUAL
import org.apache.log4j.Logger
import java.util.HashMap


class AzureWalletDatabaseAccessor : WalletDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureWalletDatabaseAccessor::class.java.name)
    }

    val accountTable: CloudTable
    val operationsTable: CloudTable
    val externalOperationsTable: CloudTable
    val assetsTable: CloudTable

    private val ASSET_PAIR = "AssetPair"
    private val PARTITION_KEY = "PartitionKey"
    private val CLIENT_BALANCE = "ClientBalance"

    constructor(balancesConfig: String, dictsConfig: String) {
        this.accountTable = getOrCreateTable(balancesConfig, "Accounts")
        this.operationsTable = getOrCreateTable(balancesConfig, "OperationsCash")
        this.externalOperationsTable = getOrCreateTable(balancesConfig, "ExternalOperationsCash")
        this.assetsTable = getOrCreateTable(dictsConfig, "Dictionaries")
    }

    override fun loadBalances(): HashMap<String, MutableMap<String, Double>> {
        val result = HashMap<String, MutableMap<String, Double>>()
        try {
            val partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, CLIENT_BALANCE)
            val partitionQuery = TableQuery.from(Wallet::class.java).where(partitionFilter)

            accountTable.execute(partitionQuery).forEach { wallet ->
                val map = result.getOrPut(wallet.rowKey) { HashMap<String, Double>() }
                wallet.getBalances().forEach { balance ->
                    map.put(balance.Asset, balance.Balance)
                }
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load balances", e)
        }

        return result
    }

    override fun loadWallets(): HashMap<String, Wallet> {
        val result = HashMap<String, Wallet>()
        try {
            val partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, CLIENT_BALANCE)
            val partitionQuery = TableQuery.from(Wallet::class.java).where(partitionFilter)

            accountTable.execute(partitionQuery).forEach { wallet ->
                result.put(wallet.rowKey, wallet)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load accounts", e)
        }

        return result
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        try {
            batchInsertOrMerge(accountTable, wallets)
        } catch(e: Exception) {
            LOGGER.error("Unable to update accounts, size: ${wallets.size}", e)
        }
    }


    override fun insertExternalCashOperation(operation: ExternalCashOperation) {
        try {
            externalOperationsTable.execute(TableOperation.insert(operation))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert external operation: ${operation.rowKey}", e)
        }
    }

    override fun loadExternalCashOperation(clientId: String, operationId: String): ExternalCashOperation? {
        try {
            val retrieveOperation = TableOperation.retrieve(clientId, operationId, ExternalCashOperation::class.java)
            val operation = externalOperationsTable.execute(retrieveOperation).getResultAsType<ExternalCashOperation>()
            return operation
        } catch(e: Exception) {
            LOGGER.error("Unable to check if operation processed: $clientId, $operationId", e)
        }
        return null
    }

    override fun insertOperation(operation: WalletOperation) {
        try {
            operationsTable.execute(TableOperation.insert(operation))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert operation: ${operation.getUid()}", e)
        }
    }

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        val result = HashMap<String, AssetPair>()

        try {
            val partitionQuery = TableQuery.from(AssetPair::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", EQUAL, ASSET_PAIR))

            for (asset in assetsTable.execute(partitionQuery)) {
                result[asset.getAssetPairId()] = asset
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset pairs", e)
        }

        return result
    }

    override fun loadAssetPair(assetId: String): AssetPair? {
        try {
            val retrieveAssetPair = TableOperation.retrieve(ASSET_PAIR, assetId, AssetPair::class.java)
            return assetsTable.execute(retrieveAssetPair).getResultAsType<AssetPair>()
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset: $assetId", e)
        }
        return null
    }
}