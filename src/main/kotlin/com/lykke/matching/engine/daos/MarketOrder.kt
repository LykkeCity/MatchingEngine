package com.lykke.matching.engine.daos

import java.util.Date

class MarketOrder: Order {
    //partition key: client_id
    //row key: uid

    var price: Double? = null
    //date of execution
    var matchedAt: Date? = null

    constructor() {}

    constructor(uid: String, assetPairId: String, clientId: String,
                price: Double? = null, createdAt: Date, registered: Date, status: String, volume: Double,
                transactionId: String? = null) : super(uid, assetPairId, clientId, createdAt, registered, status, volume, transactionId) {
        this.price = price
    }

    override fun toString(): String{
        return "MarketOrder(id=$rowKey, assetPairId='$assetPairId', clientId='$partitionKey', price=$price, volume=$volume, status='$status', createdAt=$createdAt, registered=$registered, matchedAt=$matchedAt"
    }
}