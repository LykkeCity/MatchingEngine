package com.lykke.matching.engine.daos

import java.util.Date

class LimitOrder: Order {
    //partition key: client_id
    //row key: uid

    var price: Double = 0.0
    var remainingVolume: Double = 0.0
    //date of execution
    var lastMatchTime: Date? = null

    constructor() {}

    constructor(uid: String, assetPairId: String, clientId: String, lastMatchTime: Date? = null,
                price: Double, createdAt: Date, registered: Date, status: String, volume: Double,
                remainingVolume: Double, transactionId: String? = null) : super(uid, assetPairId, clientId, createdAt, registered, status, volume, transactionId) {
        this.lastMatchTime = lastMatchTime
        this.price = price
        this.remainingVolume = remainingVolume
    }

    fun getAbsRemainingVolume(): Double {
        return Math.abs(remainingVolume)
    }

    override fun toString(): String{
        return "LimitOrder(id=$rowKey, assetPair='$assetPairId', clientId='$clientId', price=$price, volume=$volume, remainingVolume=$remainingVolume, status='$status', createdAt=$createdAt, registered=$registered, lastMatchTime=$lastMatchTime)"
    }
}