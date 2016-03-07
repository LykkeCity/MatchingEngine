package com.lykke.matching.engine.daos

import com.google.gson.Gson
import java.util.ArrayList
import java.util.Date

class MarketOrder: Order {
    //partition key: Asset_Side
    //row key: uid

    var price: Double? = null
    //date of execution
    var matchedAt: Date? = null
    var matchedOrders: String? = null

    constructor() {}

    constructor(uid: String, assetPairId: String, clientId: String,
                price: Double? = null, createdAt: Date, registered: Date, status: String, volume: Double,
                matchedOrders: String? = null) : super(uid, assetPairId, clientId, createdAt, registered, status, volume) {
        this.price = price
        this.matchedOrders = matchedOrders
    }

    fun loadMatchedOrdersList(): MutableList<MatchedLimitOrder> {
        var result:MutableList<MatchedLimitOrder> = ArrayList()
        if (matchedOrders != null) {
            result.addAll(Gson().fromJson(matchedOrders, Array<MatchedLimitOrder>::class.java).asList())
        }

        return result
    }

    fun addMatchedOrder(order: MatchedLimitOrder) {
        val orders = loadMatchedOrdersList()
        orders.add(order)
        saveMatchedOrdersList(orders)
    }

    fun saveMatchedOrdersList(orders: List<MatchedLimitOrder>) {
        this.matchedOrders = Gson().toJson(orders)
    }

    override fun toString(): String{
        return "MarketOrder(id=$rowKey, assetPairId='$assetPairId', clientId='$partitionKey', price=$price, volume=$volume, status='$status', createdAt=$createdAt, registered=$registered, matchedAt=$matchedAt, matchedOrders=$matchedOrders)"
    }
}

class MatchedLimitOrder(var Price: Double, var Id: String, var Volume: Double)