package com.lykke.matching.engine.utils.config

import com.google.gson.annotations.SerializedName

data class MatchingEngineConfig(
    @SerializedName("IpEndpoint")
    val socket: IpEndpoint,
    val metricLoggerSize: Int,
    val metricLoggerKeyValue: String,
    val metricLoggerLine: String,
    val serverOrderBookPort: Int,
    val serverOrderBookMaxConnections: Int,
    @SerializedName("RabbitMq")
    val rabbit: RabbitConfig,
    val bestPricesInterval: Long,
    val candleSaverInterval: Long,
    val hoursCandleSaverInterval: Long,
    val queueSizeLoggerInterval: Long,
    val lykkeTradesHistoryEnabled: Boolean,
    val lykkeTradesHistoryAssets: String,
    val whiteList: String?,
    val backendQueueName: String?,
    val serviceBusConnectionString: String,
    val migrate: Boolean,
    val useFileOrderBook: Boolean,
    val orderBookPath: String,
    val publishToRabbitQueue: Boolean
)