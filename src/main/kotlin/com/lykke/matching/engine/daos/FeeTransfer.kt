package com.lykke.matching.engine.daos

import java.math.BigDecimal
import java.util.Date

data class FeeTransfer(
        val externalId: String?,
        val fromClientId: String,
        val toClientId: String,
        val dateTime: Date,
        val volume: BigDecimal,
        val asset: String,
        val feeCoef: BigDecimal?
)