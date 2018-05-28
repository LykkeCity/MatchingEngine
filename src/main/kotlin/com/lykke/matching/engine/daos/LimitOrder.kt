package com.lykke.matching.engine.daos

import java.io.Serializable
import java.math.BigDecimal
import java.util.Date

class LimitOrder(id: String, uid: String, assetPairId: String, clientId: String, volume: BigDecimal, var price: BigDecimal,
                 status: String, createdAt: Date, registered: Date, var remainingVolume: BigDecimal, var lastMatchTime: Date?)
    : Order(id, uid, assetPairId, clientId, volume, status, createdAt, registered), Serializable {

    fun getAbsRemainingVolume(): BigDecimal {
        return remainingVolume.abs()
    }
}