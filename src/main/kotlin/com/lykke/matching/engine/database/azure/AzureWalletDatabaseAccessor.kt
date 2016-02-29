package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons.EQUAL
import java.util.HashMap
import java.util.Properties


class AzureWalletDatabaseAccessor : WalletDatabaseAccessor {

    val accountTable: CloudTable
    val operationsTable: CloudTable
    val assetsTable: CloudTable

    private val ASSET_PAIR = "AssetPair"

    constructor(config: Properties) {
        val storageConnectionString =
                "DefaultEndpointsProtocol=${config.getProperty("azure.default.endpoints.protocol")};" +
                "AccountName=${config.getProperty("azure.account.name")};" +
                "AccountKey=${config.getProperty("azure.account.key")}"
        val storageAccount = CloudStorageAccount.parse(storageConnectionString)
        val tableClient = storageAccount.createCloudTableClient()

        this.accountTable = tableClient.getTableReference("Accounts")
        this.operationsTable = tableClient.getTableReference("OperationsCash")
        this.assetsTable = tableClient.getTableReference("Dictionaries")
    }

    override fun loadWallets(): HashMap<String, MutableMap<String, Wallet>> {
        val result = HashMap<String, MutableMap<String, Wallet>>()
        val partitionQuery = TableQuery.from(Wallet::class.java)

        for (wallet in accountTable.execute(partitionQuery)){
            val map = result.getOrPut(wallet.partitionKey) { HashMap<String, Wallet>() }
            map.put(wallet.rowKey, wallet)
        }

        return result
    }

    override fun insertOrUpdateWallet(wallet: Wallet) {
        accountTable.execute(TableOperation.insertOrMerge(wallet))
    }

    override fun deleteWallet(wallet: Wallet) {
        val retrieveWallet = TableOperation.retrieve(wallet.partitionKey, wallet.rowKey, Wallet::class.java)
        val entityWallet = accountTable.execute(retrieveWallet).getResultAsType<Wallet>()

        val deleteWallet = TableOperation.delete(entityWallet)
        accountTable.execute(deleteWallet)
    }

    override fun addOperation(operation: WalletOperation) {
        operationsTable.execute(TableOperation.insertOrMerge(operation))
    }

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        val result = HashMap<String, AssetPair>()
        val partitionQuery = TableQuery.from(AssetPair::class.java)
                .where(TableQuery.generateFilterCondition("PartitionKey", EQUAL, ASSET_PAIR))

        for (asset in assetsTable.execute(partitionQuery)) {
            result[asset.getAssetPairId()] = asset
        }

        return result
    }

    override fun loadAssetPair(assetId: String): AssetPair? {
        val retrieveAssetPair = TableOperation.retrieve(ASSET_PAIR, assetId, AssetPair::class.java)
        return accountTable.execute(retrieveAssetPair).getResultAsType<AssetPair>()
    }
}