package com.lykke.matching.engine.daos

import com.google.gson.Gson
import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.ArrayList
import java.util.Date

class MarketOrder: TableServiceEntity {
    //partition key: Asset_Side
    //row key: uid

    var assetPair: String = ""
    var orderType: String = ""
    var blockChain: String = ""
    var price: Double? = null
    var volume: Double = 0.0
    var status: String = ""
    //date from incoming message
    var createdAt: Date = Date()
    //date of registering by matching engine
    var registered: Date = Date()
    //date of execution
    var matchedAt: Date? = null
    var matchedOrders: String? = null

    constructor() {}

    constructor(rowKey: String, assetPair: String, clientId: String, matchedAt: Date?, orderType: String,
                blockChain: String,createdAt: Date, registered: Date, status: String, volume: Double,
                matchedOrders: String?) : super(clientId, rowKey) {
        this.assetPair = assetPair
        this.matchedAt = matchedAt
        this.orderType = orderType
        this.blockChain = blockChain
        this.createdAt = createdAt
        this.registered = registered
        this.status = status
        this.volume = volume
        this.matchedOrders = matchedOrders
    }

    fun getId() = rowKey
    fun getClientId() = partitionKey

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
        return "MarketOrder(id=$rowKey, assetPair='$assetPair', clientId='$partitionKey', orderType='$orderType', blockChain='$blockChain', price=$price, volume=$volume, status='$status', createdAt=$createdAt, registered=$registered, matchedAt=$matchedAt, matchedOrders=$matchedOrders)"
    }


}

class MatchedLimitOrder(var Price: Double, var Id: String, var Volume: Double)