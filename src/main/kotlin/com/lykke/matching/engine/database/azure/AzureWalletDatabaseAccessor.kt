package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons.EQUAL
import org.apache.log4j.Logger
import java.util.HashMap
import java.util.Properties


class AzureWalletDatabaseAccessor : WalletDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureWalletDatabaseAccessor::class.java.name)
    }


    val accountTable: CloudTable
    val operationsTable: CloudTable
    val assetsTable: CloudTable

    private val ASSET_PAIR = "AssetPair"

    constructor(config: Properties) {
        val storageConnectionString =
                "DefaultEndpointsProtocol=${config.getProperty("azure.default.endpoints.protocol")};" +
                "AccountName=${config.getProperty("azure.account.name")};" +
                "AccountKey=${config.getProperty("azure.account.key")}"

        this.accountTable = getOrCreateTable(storageConnectionString, "Accounts")
        this.operationsTable = getOrCreateTable(storageConnectionString, "OperationsCash")
        this.assetsTable = getOrCreateTable(storageConnectionString, "Dictionaries")
    }

    override fun loadBalances(): HashMap<String, MutableMap<String, Double>> {
        val result = HashMap<String, MutableMap<String, Double>>()
        try {
            val partitionQuery = TableQuery.from(Wallet::class.java)

            accountTable.execute(partitionQuery).forEach { wallet ->
                val map = result.getOrPut(wallet.rowKey) { HashMap<String, Double>() }
                wallet.getBalances().forEach { balance ->
                    map.put(balance.asset, balance.balance)
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
            val partitionQuery = TableQuery.from(Wallet::class.java)

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

    override fun insertOperations(operations: Map<String, List<WalletOperation>>) {
        try {
            operations.values.forEach { clientOperations ->
                batchInsertOrMerge(operationsTable, clientOperations)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to insert operations, size: ${operations.size}", e)
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
            return accountTable.execute(retrieveAssetPair).getResultAsType<AssetPair>()
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset: $assetId", e)
        }
        return null
    }
}