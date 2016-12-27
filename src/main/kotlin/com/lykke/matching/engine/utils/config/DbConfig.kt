package com.lykke.matching.engine.utils.config

data class DbConfig(
    val clientPersonalInfoConnString: String,
    val balancesInfoConnString: String,
    val aLimitOrdersConnString: String,
    val hLimitOrdersConnString: String,
    val hMarketOrdersConnString: String,
    val hTradesConnString: String,
    val hLiquidityConnString: String,
    val backOfficeConnString: String,
    val logsConnString: String,
    val dictsConnString: String,
    val bitCoinQueueConnectionString: String,
    val metricsConnectionString: String,
    val olapLogsConnString: String,
    val olapConnString: String,
    val marketMakerConnString: String,
    val sharedStorageConnString: String,
    val limitOrdersConnString: String,
    val bitcoinHandlerConnString: String,
    val solarCoinConnString: String
)