package com.lykke.matching.engine.daos

import java.math.BigDecimal
import java.util.Date

data class WalletOperation(
        val id: String,
        val externalId: String?,
        val clientId: String,
        val assetId: String,
        val dateTime: Date,
        val amount: BigDecimal,
        val reservedAmount: BigDecimal = BigDecimal.ZERO,
        val isFee: Boolean = false
)