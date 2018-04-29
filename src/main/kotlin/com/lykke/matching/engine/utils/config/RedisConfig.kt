package com.lykke.matching.engine.utils.config

data class RedisConfig(val balancesHost: String,
                       val balancesPort: Int,
                       val balancesDbIndex: Int)