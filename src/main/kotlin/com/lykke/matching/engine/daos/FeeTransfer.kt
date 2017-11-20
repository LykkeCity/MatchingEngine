package com.lykke.matching.engine.daos

import java.util.Date

data class FeeTransfer(
        val externalId: String?,
        val fromClientId: String,
        val toClientId: String,
        val dateTime: Date,
        val volume: Double,
        var asset: String
)