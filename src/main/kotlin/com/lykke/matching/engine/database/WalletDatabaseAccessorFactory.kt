package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.file.FileWalletDatabaseAccessor
import com.lykke.matching.engine.utils.config.MatchingEngineConfig

fun createWalletDatabaseAccessor(config: MatchingEngineConfig): WalletDatabaseAccessor {
    return when (config.walletsStorage) {
        WalletsStorage.Azure -> AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, config.db.dictsConnString)
        WalletsStorage.File -> FileWalletDatabaseAccessor(config.walletsPath, AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, config.db.dictsConnString))
    }
}