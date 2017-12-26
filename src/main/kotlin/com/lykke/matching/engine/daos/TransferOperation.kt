package com.lykke.matching.engine.daos

import java.util.Date

data class TransferOperation(
        val id: String,
        val externalId: String,
        val fromClientId: String,
        val toClientId: String,
        val asset: String,
        val dateTime: Date,
        val volume: Double,
        val overdraftLimit: Double?,
        val fees: List<FeeInstruction>?)