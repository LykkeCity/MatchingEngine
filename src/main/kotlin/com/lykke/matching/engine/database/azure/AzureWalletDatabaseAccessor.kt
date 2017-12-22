package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.azure.AzureAssetPair
import com.lykke.matching.engine.daos.azure.AzureWalletSwapOperation
import com.lykke.matching.engine.daos.azure.AzureWalletTransferOperation
import com.lykke.matching.engine.daos.azure.wallet.AzureWallet
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons.EQUAL
import java.util.HashMap


class AzureWalletDatabaseAccessor(balancesConfig: String, dictsConfig: String) : WalletDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(AzureWalletDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val accountTable: CloudTable = getOrCreateTable(balancesConfig, "Accounts")
    private val transferOperationsTable: CloudTable = getOrCreateTable(balancesConfig, "SwapOperationsCash")
    private val assetsTable: CloudTable = getOrCreateTable(dictsConfig, "Dictionaries")

    private val ASSET_PAIR = "AssetPair"
    private val PARTITION_KEY = "PartitionKey"
    private val CLIENT_BALANCE = "ClientBalance"

    override fun loadBalances(): HashMap<String, MutableMap<String, AssetBalance>> {
        val result = HashMap<String, MutableMap<String, AssetBalance>>()
        var balancesCount = 0
        try {
            val partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, CLIENT_BALANCE)
            val partitionQuery = TableQuery.from(AzureWallet::class.java).where(partitionFilter)

            accountTable.execute(partitionQuery).forEach { wallet ->
                val map = result.getOrPut(wallet.rowKey) { HashMap() }
                wallet.balancesList.forEach { balance ->
                    if (balance.balance != null) {
                        map.put(balance.asset, AssetBalance(balance.asset, balance.balance, balance.reserved ?: 0.0))
                        balancesCount++
                    }
                }
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load balances", e)
            METRICS_LOGGER.logError( "Unable to load balances", e)
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
                result.put(wallet.rowKey, Wallet(wallet.clientId, wallet.balancesList.map { AssetBalance(it.asset, it.balance, it.reserved ?: 0.0)}))
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load accounts", e)
            METRICS_LOGGER.logError( "Unable to load accounts", e)
        }

        return result
    }

    override fun insertOrUpdateWallets(wallets: List<Wallet>) {
        try {
            batchInsertOrMerge(accountTable, wallets.map { AzureWallet(it.clientId, it.balances.map { it.value })})
        } catch(e: Exception) {
            LOGGER.error("Unable to update accounts, size: ${wallets.size}", e)
            METRICS_LOGGER.logError( "Unable to update accounts, size: ${wallets.size}", e)
        }
    }

    override fun insertTransferOperation (operation: TransferOperation) {
        try {
            transferOperationsTable.execute(TableOperation.insertOrMerge(AzureWalletTransferOperation(operation.id, operation.externalId, operation.fromClientId, operation.toClientId, operation.asset, operation.dateTime, operation.volume)))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert operation: ${operation.id}, external id: ${operation.externalId}", e)
            METRICS_LOGGER.logError( "Unable to insert operation: ${operation.id}, external id: ${operation.externalId}", e)
        }
    }

    override fun insertSwapOperation (operation: SwapOperation) {
        try {
            transferOperationsTable.execute(TableOperation.insertOrMerge(AzureWalletSwapOperation(operation.id, operation.externalId, operation.clientId1, operation.asset1, operation.volume1, operation.clientId2, operation.asset2, operation.volume2, operation.dateTime)))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert swap operation: ${operation.id}, external id: ${operation.externalId}", e)
            METRICS_LOGGER.logError( "Unable to insert swap operation: ${operation.id}, external id: ${operation.externalId}", e)
        }
    }

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        val result = HashMap<String, AssetPair>()

        try {
            val partitionQuery = TableQuery.from(AzureAssetPair::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", EQUAL, ASSET_PAIR))

            for (asset in assetsTable.execute(partitionQuery)) {
                result[asset.assetPairId] = AssetPair(asset.assetPairId, asset.baseAssetId, asset.quotingAssetId, asset.accuracy)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset pairs", e)
            METRICS_LOGGER.logError( "Unable to load asset pairs", e)
        }

        return result
    }

    override fun loadAssetPair(assetId: String): AssetPair? {
        try {
            val retrieveAssetPair = TableOperation.retrieve(ASSET_PAIR, assetId, AzureAssetPair::class.java)
            val assetPair = assetsTable.execute(retrieveAssetPair).getResultAsType<AzureAssetPair>()
            if (assetPair != null) {
                return AssetPair(assetPair.assetPairId, assetPair.baseAssetId, assetPair.quotingAssetId, assetPair.accuracy)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load asset: $assetId", e)
            METRICS_LOGGER.logError( "Unable to load asset: $assetId", e)
        }
        return null
    }
}