package com.lykke.matching.engine.outgoing.messages

import java.util.Date

class TradeInfo(
    val volume: Double,
    val price: Double,
    val limitOrderId: String,
    val limitOrderExternalId: String,
    val timestamp: Date
)