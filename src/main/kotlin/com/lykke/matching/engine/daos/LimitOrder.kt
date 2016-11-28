package com.lykke.matching.engine.daos

import java.util.Date

class LimitOrder(id: String, uid: String, assetPairId: String, clientId: String, volume: Double, var price: Double,
                 status: String, createdAt: Date, registered: Date, var remainingVolume: Double, var lastMatchTime: Date?)
    : Order(id, uid, assetPairId, clientId, volume, status, createdAt, registered) {

    fun getAbsRemainingVolume(): Double {
        return Math.abs(remainingVolume)
    }
}