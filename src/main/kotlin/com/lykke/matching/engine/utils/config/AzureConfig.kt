package com.lykke.matching.engine.utils.config

import com.google.gson.annotations.SerializedName

data class AzureConfig (
    val db: DbConfig,
    @SerializedName("MatchingEngine")
    val me: MatchingEngineConfig,
    val slackNotificationsQueueName: String?
)