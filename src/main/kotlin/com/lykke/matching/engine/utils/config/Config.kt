package com.lykke.matching.engine.utils.config

import com.google.gson.annotations.SerializedName
import com.lykke.utils.keepalive.http.KeepAliveConfig
import com.lykke.utils.logging.config.SlackNotificationConfig
import com.lykke.utils.logging.config.ThrottlingLoggerConfig

data class Config(
    @SerializedName("MatchingEngine")
    val me: MatchingEngineConfig,
    val slackNotifications: SlackNotificationConfig,
    val keepAlive: KeepAliveConfig,
    val throttlingLogger: ThrottlingLoggerConfig
)