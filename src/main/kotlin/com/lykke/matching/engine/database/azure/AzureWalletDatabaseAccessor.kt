package com.lykke.matching.engine.database.azure

import com.lykke.matching.engine.daos.azure.wallet.AzureWallet
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableQuery
import java.util.HashMap


class AzureWalletDatabaseAccessor(balancesConfig: String, balancesTableName: String) : WalletDatabaseAccessor {

    companion object {
        val LOGGER = ThrottlingLogger.getLogger(AzureWalletDatabaseAccessor::class.java.name)
        val METRICS_LOGGER = MetricsLogger.getLogger()
        const val DEFAULT_BALANCES_TABLE_NAME = "Accounts"
    }

    private val accountTable: CloudTable = getOrCreateTable(balancesConfig, balancesTableName)

    private val PARTITION_KEY = "PartitionKey"
    private val CLIENT_BALANCE = "ClientBalance"

    override fun loadWallets(): HashMap<String, Wallet> {
        val result = HashMap<String, Wallet>()
        try {
            val partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, TableQuery.QueryComparisons.EQUAL, CLIENT_BALANCE)
            val partitionQuery = TableQuery.from(AzureWallet::class.java).where(partitionFilter)

            accountTable.execute(partitionQuery).forEach { wallet ->
                result.put(wallet.rowKey, Wallet(wallet.clientId, wallet.balancesList.map {
                    AssetBalance(wallet.clientId,
                            it.asset,
                            it.balance.toBigDecimal(),
                            (it.reserved ?: 0.0).toBigDecimal())
                }))
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
}