package com.lykke.matching.engine.utils.config

import com.google.gson.annotations.SerializedName

data class Config(
    @SerializedName("MatchingEngine")
    val me: MatchingEngineConfig,
    val slackNotifications: SlackNotificationConfig
)