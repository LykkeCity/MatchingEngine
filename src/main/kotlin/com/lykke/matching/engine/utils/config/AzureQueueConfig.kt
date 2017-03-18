package com.lykke.matching.engine.utils.config

data class AzureQueueConfig(
    val connectionString: String,
    val queueName: String
)