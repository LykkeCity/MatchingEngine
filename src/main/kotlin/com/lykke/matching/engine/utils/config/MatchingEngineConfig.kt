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
    @SerializedName("RabbitMq")
    val rabbit: RabbitConfig,
    val bestPricesInterval: Long,
    val candleSaverInterval: Long,
    val hoursCandleSaverInterval: Long,
    val queueSizeLoggerInterval: Long,
    val lykkeTradesHistoryEnabled: Boolean,
    val whiteList: String?,
    val backendQueueName: String?,
    val migrate: Boolean,
    val useFileOrderBook: Boolean,
    val orderBookPath: String,
    val publishToRabbitQueue: Boolean,
    val queueSizeLimit: Int
)