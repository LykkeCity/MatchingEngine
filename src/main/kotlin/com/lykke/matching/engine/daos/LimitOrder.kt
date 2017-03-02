package com.lykke.matching.engine.daos

import java.io.Serializable
import java.util.Date

class LimitOrder(id: String, externalId: String, assetPairId: String, clientId: String, volume: Double, var price: Double,
                 status: String, createdAt: Date, registered: Date, var remainingVolume: Double, var lastMatchTime: Date?)
    : Order(id, externalId, assetPairId, clientId, volume, status, createdAt, registered), Serializable {

    fun getAbsRemainingVolume(): Double {
        return Math.abs(remainingVolume)
    }
}