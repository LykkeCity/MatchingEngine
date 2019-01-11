package com.lykke.matching.engine.utils.config

data class IpEndpoint(
    val port: Int,
    val maxConnections: Int,
    val lifeTimeMinutes: Long?
)