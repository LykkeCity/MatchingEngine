package com.lykke.matching.engine.outgoing.messages

import com.lykke.matching.engine.daos.LimitOrder
import java.util.ArrayList
import java.util.Date
import java.util.concurrent.PriorityBlockingQueue

class OrderBook: JsonSerializable {

    val assetPair: String
    val isBuy: Boolean
    val timestamp: Date

    val prices: MutableList<Order> = ArrayList()

    constructor(assetPair: String, isBuy: Boolean, timestamp: Date) {
        this.assetPair = assetPair
        this.isBuy = isBuy
        this.timestamp = timestamp
    }

    constructor(assetPair: String, isBuy: Boolean, timestamp: Date, orders: PriorityBlockingQueue<LimitOrder>) {
        this.assetPair = assetPair
        this.isBuy = isBuy
        this.timestamp = timestamp

        while (!orders.isEmpty()) {
            val order = orders.poll()
            addVolumePrice(order.externalId, order.clientId, order.remainingVolume, order.price)
        }
    }

    fun addVolumePrice(id: String, clientId: String, volume: Double, price: Double) {
        prices.add(Order(id, clientId, volume, price))
    }
}

class Order(val id: String, val clientId: String, val volume: Double, val price: Double)
