package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.BtTransaction
import com.lykke.matching.engine.daos.WalletCredentials
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import org.apache.log4j.Logger


class AzureBackOfficeDatabaseAccessor : BackOfficeDatabaseAccessor {

    companion object {
        val LOGGER = Logger.getLogger(AzureWalletDatabaseAccessor::class.java.name)
    }

    val walletCredentialsTable: CloudTable
    val bitcoinTransactionTable: CloudTable
    val assetsTable: CloudTable

    private val WALLET = "Wallet"
    private val ASSET = "Asset"

    constructor(сlientPersonalInfoString: String?, bitCoinQueueString: String?, dictsConfig: String?) {
        this.walletCredentialsTable = getOrCreateTable(сlientPersonalInfoString!!, "WalletCredentials")
        this.bitcoinTransactionTable = getOrCreateTable(bitCoinQueueString!!, "BitCoinTransactions")
        this.assetsTable = getOrCreateTable(dictsConfig!!, "Dictionaries")
    }

    override fun loadWalletCredentials(clientId: String): WalletCredentials? {
        try {
            val retrieveWalletCredentials = TableOperation.retrieve(WALLET, clientId, WalletCredentials::class.java)
            return walletCredentialsTable.execute(retrieveWalletCredentials).getResultAsType<WalletCredentials>()
        } catch(e: Exception) {
            LOGGER.error("Unable to load wallet credentials: $clientId", e)
        }
        return null
    }

    override fun loadAsset(assetId: String): Asset? {
        try {
            val retrieveAssetAsset = TableOperation.retrieve(ASSET, assetId, Asset::class.java)
            return assetsTable.execute(retrieveAssetAsset).getResultAsType<Asset>()
        } catch(e: Exception) {
            LOGGER.error("Unable to load assetId: $assetId", e)
        }
        return null
    }

    override fun saveBitcoinTransaction(transaction: BtTransaction) {
        try {
            bitcoinTransactionTable.execute(TableOperation.insert(transaction))
        } catch(e: Exception) {
            LOGGER.error("Unable to insert bitcoin transaction: ${transaction.rowKey}", e)
        }
    }
}