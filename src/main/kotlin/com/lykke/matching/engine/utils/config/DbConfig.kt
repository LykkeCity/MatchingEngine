package com.lykke.matching.engine.utils.config

data class DbConfig(
    val clientPersonalInfoConnString: String,
    val balancesInfoConnString: String,
    val aLimitOrdersConnString: String,
    val hLimitOrdersConnString: String,
    val hMarketOrdersConnString: String,
    val hTradesConnString: String,
    val hLiquidityConnString: String,
    val dictsConnString: String,
    val bitCoinQueueConnectionString: String,
    val sharedStorageConnString: String
)