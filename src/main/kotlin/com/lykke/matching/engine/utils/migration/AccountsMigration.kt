package com.lykke.matching.engine.utils.migration

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.WalletsStorage
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.file.FileWalletDatabaseAccessor
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.MatchingEngineConfig
import org.apache.log4j.Logger
import java.io.File
import java.util.Date
import java.util.LinkedList

fun migrateAccountsIfConfigured(config: Config) {
    if (!config.me.walletsMigration) {
        return
    }
    when (config.me.walletsStorage) {
        WalletsStorage.File -> AccountsMigration(config.me).fromDbToFile()
        WalletsStorage.Azure -> AccountsMigration(config.me).fromFileToDb()
    }
}

class AccountsMigrationException(message: String) : Exception(message)

class AccountsMigration(config: MatchingEngineConfig) {
    companion object {
        private val LOGGER = Logger.getLogger(AccountsMigration::class.java.name)
    }

    private val azureAccountsTableName = config.db.accountsTableName ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME
    private val fileAccountsPath = config.fileDb.walletsPath
    private val azureDatabaseAccessor = AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, azureAccountsTableName)
    private val fileDatabaseAccessor = FileWalletDatabaseAccessor(fileAccountsPath)

    fun fromDbToFile() {
        if (File(fileAccountsPath).listFiles().any { it.isFile }) {
            throw AccountsMigrationException("Accounts files already exist in $fileAccountsPath")
        }

        val startTime = Date().time
        teeLog("Starting wallets migration from azure to files; azure table: $azureAccountsTableName, files path: $fileAccountsPath")
        val wallets = azureDatabaseAccessor.loadWallets()
        val loadTime = Date().time
        teeLog("Loaded ${wallets.size} wallets from azure (ms: ${loadTime - startTime})")
        fileDatabaseAccessor.insertOrUpdateWallets(wallets.values.toList())
        val saveTime = Date().time
        teeLog("Saved ${wallets.size} wallets to files (ms: ${saveTime - loadTime})")

        compare()
    }

    fun fromFileToDb() {
        if (!File(fileAccountsPath).listFiles().any { it.isFile }) {
            throw AccountsMigrationException("There are no accounts files in $fileAccountsPath")
        }

        val startTime = Date().time
        teeLog("Starting wallets migration from files to azure; files path: $fileAccountsPath, azure table: $azureAccountsTableName")
        val wallets = fileDatabaseAccessor.loadWallets()
        val loadTime = Date().time
        teeLog("Loaded ${wallets.size} wallets from files (ms: ${loadTime - startTime})")
        azureDatabaseAccessor.insertOrUpdateWallets(wallets.values.toList())
        val saveTime = Date().time
        teeLog("Saved ${wallets.size} wallets to azure (ms: ${saveTime - loadTime})")

        compare()
    }

    private fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }

    /** Compares balances stored in files & azure; logs comparison result  */
    private fun compare() {
        val azureWallets = azureDatabaseAccessor.loadWallets()
        val fileWallets = fileDatabaseAccessor.loadWallets()

        val onlyAzureClients = azureWallets.keys.filterNot { fileWallets.contains(it) }
        val onlyFileClients = fileWallets.keys.filterNot { azureWallets.contains(it) }
        val commonClients = azureWallets.keys.filter { fileWallets.contains(it) }

        val differentWallets = LinkedList<String>()

        teeLog("Comparison result. Differences: ")
        teeLog("---------------------------------------------------------------------------------------------")
        commonClients.forEach {
            val azureWallet = azureWallets[it]
            val fileWallet = fileWallets[it]
            if (!compareBalances(azureWallet!!, fileWallet!!)) {
                differentWallets.add(it)
            }
        }
        teeLog("---------------------------------------------------------------------------------------------")

        teeLog("Total: ")
        teeLog("only azure clients (count: ${onlyAzureClients.size}): $onlyAzureClients")
        teeLog("only file clients (count: ${onlyFileClients.size}): $onlyFileClients")
        teeLog("clients with different wallets (count: ${differentWallets.size}): $differentWallets")
    }

    private fun compareBalances(azureWallet: Wallet, fileWallet: Wallet): Boolean {
        if (azureWallet.clientId != fileWallet.clientId) {
            teeLog("different clients: ${azureWallet.clientId} & ${fileWallet.clientId}")
            return false
        }
        val clientId = azureWallet.clientId
        val azureBalances = azureWallet.balances
        val fileBalances = fileWallet.balances

        val onlyAzureAssets = azureBalances.keys.filterNot { fileBalances.keys.contains(it) || azureBalances[it]!!.balance == 0.0 }
        val onlyFileAssets = fileBalances.keys.filterNot { azureBalances.keys.contains(it) }

        if (onlyAzureAssets.isNotEmpty() || onlyFileAssets.isNotEmpty()) {
            teeLog("different asset sets: $onlyAzureAssets & $onlyFileAssets, client: $clientId")
            return false
        }

        val commonAssets = fileBalances.keys.filter { azureBalances.keys.contains(it) }
        commonAssets.forEach {
            val azureBalance = azureBalances[it]
            val fileBalance = fileBalances[it]
            if (azureBalance!!.balance != fileBalance!!.balance) {
                teeLog("different balances: ${azureBalance.balance} & ${fileBalance.balance}, client: $clientId")
                return false
            }
            if (azureBalance.reserved != fileBalance.reserved) {
                teeLog("different reserved balances: ${azureBalance.reserved} & ${fileBalance.reserved}, client: $clientId")
                return false
            }
        }

        return true
    }
}