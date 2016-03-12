package com.lykke.matching.engine.daos

import com.google.gson.Gson
import java.util.*

class LimitOrder: Order {
    //partition key: Asset_Side
    //row key: uid

    var price: Double = 0.0
    var remainingVolume: Double = 0.0
    //date of execution
    var lastMatchTime: Date? = null
    var matchedOrders: String? = null

    constructor() {}

    constructor(uid: String, assetPairId: String, clientId: String, lastMatchTime: Date? = null,
                price: Double, createdAt: Date, registered: Date, status: String, volume: Double,
                remainingVolume: Double, matchedOrders: String? = null, transactionId: String? = null) : super(uid, assetPairId, clientId, createdAt, registered, status, volume, transactionId) {
        this.lastMatchTime = lastMatchTime
        this.price = price
        this.remainingVolume = remainingVolume
        this.matchedOrders = matchedOrders
    }

    fun loadMatchedOrders(): MutableList<MatchedMarketOrder> {
        var result:MutableList<MatchedMarketOrder> = ArrayList()
        if (matchedOrders != null) {
            result.addAll(Gson().fromJson(matchedOrders, Array<MatchedMarketOrder>::class.java).asList())
        }

        return result
    }

    fun addMatchedOrder(order: MatchedMarketOrder) {
        val orders = loadMatchedOrders()
        orders.add(order)
        saveMatchedOrdersList(orders)
    }

    fun saveMatchedOrdersList(orders: List<MatchedMarketOrder>) {
        this.matchedOrders = Gson().toJson(orders)
    }

    override fun toString(): String{
        return "LimitOrder(id=$rowKey, assetPair='$assetPairId', clientId='$clientId', price=$price, volume=$volume, remainingVolume=$remainingVolume, status='$status', createdAt=$createdAt, registered=$registered, lastMatchTime=$lastMatchTime, matchedOrders=${loadMatchedOrders()})"
    }


}

class MatchedMarketOrder(var Id: String, var Volume: Double)