package com.lykke.matching.engine.daos

import java.math.BigDecimal
import java.util.Date

data class TransferOperation(
        val id: String,
        val externalId: String,
        val fromClientId: String,
        val toClientId: String,
        val asset: String,
        val dateTime: Date,
        val volume: BigDecimal,
        val overdraftLimit: BigDecimal?,
        val fees: List<FeeInstruction>?)