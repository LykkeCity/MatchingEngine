package com.lykke.matching.engine.utils.config

data class RedisConfig(
    val host: String,
    val port: Int,
    val timeout: Int,
    val useSsl: Boolean,
    val password: String?,
    val balanceDatabase: Int,
    val processedMessageDatabase: Int,
    val processedCashMessageDatabase: Int,
    val pingDatabase: Int,
    val sequenceNumberDatabase: Int
)