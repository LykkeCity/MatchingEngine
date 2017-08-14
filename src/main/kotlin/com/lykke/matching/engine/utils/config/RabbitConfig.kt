package com.lykke.matching.engine.utils.config

data class RabbitConfig(
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val exchangeOrderbook: String,
    val exchangeTransfer: String,
    val exchangeSwapOperation: String,
    val exchangeCashOperation: String,
    val exchangeSwap: String,
    val exchangeLimitOrders: String,
    val trustedExchangeLimitOrders: String,
    val exchangeBalanceUpdate: String
)