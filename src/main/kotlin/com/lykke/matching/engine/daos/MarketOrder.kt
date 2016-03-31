package com.lykke.matching.engine.daos

import java.util.Date

class MarketOrder: Order {
    //partition key: client_id
    //row key: uid

    var price: Double? = null
    //date of execution
    var matchedAt: Date? = null
    var straight: Boolean = true

    constructor() {}

    constructor(uid: String, assetPairId: String, clientId: String,
                price: Double? = null, createdAt: Date, registered: Date, status: String, volume: Double, straight: Boolean = true,
                transactionId: String? = null) : super(uid, assetPairId, clientId, createdAt, registered, status, volume, transactionId) {
        this.price = price
        this.straight = straight
    }

    fun isBuySide(): Boolean {
        return if (straight) isBuySide(volume) else !isBuySide(volume)
    }

    override fun toString(): String{
        return "MarketOrder(id=$rowKey, assetPairId='$assetPairId', clientId='$partitionKey', price=$price, volume=$volume, straight=$straight, status='$status', createdAt=$createdAt, registered=$registered, matchedAt=$matchedAt"
    }
}