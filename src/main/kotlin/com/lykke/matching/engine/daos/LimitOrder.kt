package com.lykke.matching.engine.daos

import com.google.gson.Gson
import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.ArrayList
import java.util.Date

class LimitOrder: TableServiceEntity {
    //partition key: Asset_Side
    //row key: uid

    var assetPair: String = ""
    var clientId: String = ""
    var orderType: String = ""
    var blockChain: String = ""
    var price: Double = 0.0
    var volume: Double = 0.0
    var remainingVolume: Double = 0.0
    var status: String = ""
    //date from incoming message
    var createdAt: Date = Date()
    //date of registering by matching engine
    var registered: Date = Date()
    //date of execution
    var lastMatchTime: Date? = null
    var matchedOrders: String? = null

    constructor() {}

    constructor(partitionKey: String, rowKey: String, assetId: String, clientId: String, lastMatchTime: Date?,
                orderType: String, blockChain: String, price: Double, createdAt: Date, registered: Date,
                status: String, volume: Double, remainingVolume: Double, matchedOrders: String?) : super(partitionKey, rowKey) {
        this.assetPair = assetId
        this.clientId = clientId
        this.lastMatchTime = lastMatchTime
        this.orderType = orderType
        this.blockChain = blockChain
        this.price = price
        this.createdAt = createdAt
        this.registered = registered
        this.status = status
        this.volume = volume
        this.remainingVolume = remainingVolume
        this.matchedOrders = matchedOrders
    }

    fun getId() = rowKey

    fun getMatchedOrders(): MutableList<MatchedMarketOrder> {
        var result:MutableList<MatchedMarketOrder> = ArrayList()
        if (matchedOrders != null) {
            result.addAll(Gson().fromJson(matchedOrders, Array<MatchedMarketOrder>::class.java).asList())
        }

        return result
    }

    fun addMatchedOrder(order: MatchedMarketOrder) {
        val orders = getMatchedOrders()
        orders.add(order)
        setMatchedOrders(orders)
    }

    fun setMatchedOrders(orders: List<MatchedMarketOrder>) {
        this.matchedOrders = Gson().toJson(orders)
    }

    override fun toString(): String{
        return "LimitOrder(id=$rowKey, assetPair='$assetPair', clientId='$clientId', orderType='$orderType', blockChain='$blockChain', price=$price, volume=$volume, remainingVolume=$remainingVolume, status='$status', createdAt=$createdAt, registered=$registered, lastMatchTime=$lastMatchTime, matchedOrders=${getMatchedOrders()})"
    }


}

class MatchedMarketOrder(var Id: String, var Volume: Double)