package com.lykke.matching.engine.utils.config

import com.google.gson.annotations.SerializedName
import com.lykke.matching.engine.database.WalletsStorage
import com.lykke.utils.alivestatus.config.AliveStatusConfig

data class MatchingEngineConfig(
    val db: DbConfig,
    val fileDb: FileDbConfig,
    @SerializedName("IpEndpoint")
    val socket: IpEndpoint,
    val serverOrderBookPort: Int?,
    val serverOrderBookMaxConnections: Int?,
    val httpOrderBookPort: Int,
    val httpBalancesPort: Int,
    val rabbitMqConfigs: RabbitMqConfigs,
    val bestPricesInterval: Long,
    val candleSaverInterval: Long,
    val hoursCandleSaverInterval: Long,
    val queueSizeLoggerInterval: Long,
    val negativeSpreadAssets: String,
    val whiteList: String?,
    val migrate: Boolean,
    val queueSizeLimit: Int,
    val name: String,
    val trustedClients: Set<String>,
    val aliveStatus: AliveStatusConfig,
    val walletsStorage: WalletsStorage,
    val walletsMigration: Boolean
)