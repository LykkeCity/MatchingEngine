package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.file.FileWalletDatabaseAccessor
import com.lykke.matching.engine.utils.config.MatchingEngineConfig

fun createWalletDatabaseAccessor(config: MatchingEngineConfig): WalletDatabaseAccessor = when (config.walletsStorage) {
    WalletsStorage.Azure -> AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, config.db.accountsTableName ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME)
    WalletsStorage.File -> FileWalletDatabaseAccessor(config.fileDb.walletsPath)
}