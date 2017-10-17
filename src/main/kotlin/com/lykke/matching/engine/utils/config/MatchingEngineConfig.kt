package com.lykke.matching.engine.utils.config

import com.google.gson.annotations.SerializedName

data class MatchingEngineConfig(
    val db: DbConfig,
    @SerializedName("IpEndpoint")
    val socket: IpEndpoint,
    val metricLoggerSize: Int,
    val metricLoggerKeyValue: String,
    val metricLoggerLine: String,
    val serverOrderBookPort: Int,
    val serverOrderBookMaxConnections: Int,
    val httpOrderBookPort: Int,
    @SerializedName("RabbitMqOrderBook")
    val rabbitOrderBook: RabbitConfig,
    @SerializedName("RabbitMqCashOperation")
    val rabbitCashOperation: RabbitConfig,
    @SerializedName("RabbitMqTransfer")
    val rabbitTransfer: RabbitConfig,
    @SerializedName("RabbitMqSwapOperation")
    val rabbitSwapOperation: RabbitConfig,
    @SerializedName("RabbitMqBalanceUpdate")
    val rabbitBalanceUpdate: RabbitConfig,
    @SerializedName("RabbitMqSwap")
    val rabbitSwap: RabbitConfig,
    @SerializedName("RabbitMqLimitOrders")
    val rabbitLimitOrders: RabbitConfig,
    @SerializedName("RabbitMqTrustedLimitOrders")
    val rabbitTrustedLimitOrders: RabbitConfig,
    val bestPricesInterval: Long,
    val candleSaverInterval: Long,
    val hoursCandleSaverInterval: Long,
    val queueSizeLoggerInterval: Long,
    val lykkeTradesHistoryEnabled: Boolean,
    val negativeSpreadAssets: String,
    val whiteList: String?,
    val backendQueueName: String?,
    val migrate: Boolean,
    val useFileOrderBook: Boolean,
    val orderBookPath: String,
    val publishToRabbitQueue: Boolean,
    val sendTrades: Boolean,
    val queueSizeLimit: Int,
    val name: String,
    val trustedClients: Set<String>
)