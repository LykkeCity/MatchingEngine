package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletCredentials
import com.lykke.matching.engine.daos.azure.AzureAsset
import com.lykke.matching.engine.daos.azure.AzureWalletCredentials
import com.lykke.matching.engine.daos.azure.bitcoin.AzureBtTransaction
import com.lykke.matching.engine.daos.bitcoin.BtTransaction
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.logging.MetricsLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import com.microsoft.azure.storage.table.TableQuery
import org.apache.log4j.Logger
import java.util.HashMap


class AzureBackOfficeDatabaseAccessor(сlientPersonalInfoString: String, bitCoinQueueString: String, dictsConfig: String) : BackOfficeDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureWalletDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    val walletCredentialsTable: CloudTable
    val bitcoinTransactionTable: CloudTable
    val assetsTable: CloudTable

    private val WALLET = "Wallet"
    private val ASSET = "Asset"

    override fun loadAllWalletCredentials(): MutableMap<String, WalletCredentials> {
        val result = HashMap<String, WalletCredentials>()

        try {
            val partitionQuery = TableQuery.from(AzureWalletCredentials::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, WALLET))

            for (wallet in walletCredentialsTable.execute(partitionQuery)) {
                result[wallet.clientId] = WalletCredentials(wallet.clientId, wallet.multiSig)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load wallet credentials", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load wallet credentials", e)
        }

        LOGGER.info("Loaded ${result.size} wallet credentials")

        return result
    }

    override fun loadWalletCredentials(clientId: String): WalletCredentials? {
        try {
            val retrieveWalletCredentials = TableOperation.retrieve(WALLET, clientId, AzureWalletCredentials::class.java)
            val wallet = walletCredentialsTable.execute(retrieveWalletCredentials).getResultAsType<AzureWalletCredentials>()
            if (wallet != null) {
                return WalletCredentials(wallet.clientId, wallet.multiSig)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load wallet credentials: $clientId", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load wallet credentials: $clientId", e)
        }
        return null
    }

    override fun loadAssets(): MutableMap<String, Asset> {
        val result = HashMap<String, Asset>()

        try {
            val partitionQuery = TableQuery.from(AzureAsset::class.java)
                    .where(TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, ASSET))

            for (asset in assetsTable.execute(partitionQuery)) {
                result[asset.assetId] = Asset(asset.assetId, asset.accuracy, asset.blockChainId, asset.dustLimit)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load assets", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load assets", e)
        }

        LOGGER.info("Loaded ${result.size} assets ")

        return result
    }

    override fun loadAsset(assetId: String): Asset? {
        try {
            val retrieveAssetAsset = TableOperation.retrieve(ASSET, assetId, AzureAsset::class.java)
            val asset = assetsTable.execute(retrieveAssetAsset).getResultAsType<AzureAsset>()
            if (asset != null) {
               return Asset(asset.assetId, asset.accuracy, asset.blockChainId, asset.dustLimit)
            }
        } catch(e: Exception) {
            LOGGER.error("Unable to load assetId: $assetId", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to load assetId: $assetId", e)
        }
        return null
    }

    override fun saveBitcoinTransaction(transaction: BtTransaction) {
        try {
            val azureBtTransaction = AzureBtTransaction(transaction.id, transaction.created, transaction.requestData,
                    transaction.clientCashOperationPair, transaction.orders)
            bitcoinTransactionTable.execute(TableOperation.insertOrMerge(azureBtTransaction))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert bitcoin transaction: $transaction", e)
            METRICS_LOGGER.logError(this.javaClass.name, "Unable to insert bitcoin transaction: $transaction", e)
        }
    }

    init {
        this.walletCredentialsTable = getOrCreateTable(сlientPersonalInfoString, "WalletCredentials")
        this.bitcoinTransactionTable = getOrCreateTable(bitCoinQueueString, "BitCoinTransactions")
        this.assetsTable = getOrCreateTable(dictsConfig, "Dictionaries")
    }
}