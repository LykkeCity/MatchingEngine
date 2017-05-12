package com.lykke.matching.engine.outgoing.messages

import java.util.Date

class TradeInfo(
    val marketClientId: String,
    val marketVolume: String,
    val marketAsset: String,
    val limitClientId: String,
    val limitVolume: String,
    val limitAsset: String,
    val price: Double,
    val limitOrderId: String,
    val limitOrderExternalId: String,
    val timestamp: Date
)