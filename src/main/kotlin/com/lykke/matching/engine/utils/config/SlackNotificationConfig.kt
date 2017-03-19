package com.lykke.matching.engine.utils.config

data class SlackNotificationConfig (
    val azureQueue: AzureQueueConfig,
    val throttlingLimitSeconds: Int
)