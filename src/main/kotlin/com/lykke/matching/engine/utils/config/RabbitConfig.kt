package com.lykke.matching.engine.utils.config

data class RabbitConfig(
    val uri: String,
    val exchange: String,
    val queueName: String? = null,
    val routingKey: String? = null
)