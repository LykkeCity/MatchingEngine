package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.file.FileWalletDatabaseAccessor
import com.lykke.matching.engine.utils.config.MatchingEngineConfig

fun createWalletDatabaseAccessor(config: MatchingEngineConfig, saveToAnotherDb: Boolean): WalletDatabaseAccessor = when (config.walletsStorage) {
    WalletsStorage.Azure -> AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, config.db.accountsTableName ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME)
    WalletsStorage.File ->  {
        val anotherDatabaseAccessor = if (saveToAnotherDb) AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, config.db.newAccountsTableName!!) else null
        FileWalletDatabaseAccessor(config.fileDb.walletsPath, anotherDatabaseAccessor)
    }
}