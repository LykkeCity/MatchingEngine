package com.lykke.matching.engine.daos

import java.math.BigDecimal
import java.util.Date

data class LkkTrade(
        val assetPair: String,
        val clientId: String,
        val price: BigDecimal,
        val volume: BigDecimal,
        val date: Date
)