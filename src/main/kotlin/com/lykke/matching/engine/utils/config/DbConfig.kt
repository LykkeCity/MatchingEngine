package com.lykke.matching.engine.utils.config

data class DbConfig(
    val balancesInfoConnString: String,
    val hTradesConnString: String,
    val hLiquidityConnString: String,
    val dictsConnString: String,
    val sharedStorageConnString: String,
    val messageLogConnString: String,
    val matchingEngineConnString: String,
    val monitoringConnString: String,
    val reservedVolumesConnString: String,
    val accountsTableName: String?,
    /** Table name for async writing of balances in case of primary redis db */
    val newAccountsTableName: String?
)