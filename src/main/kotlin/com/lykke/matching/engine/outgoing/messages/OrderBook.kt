package com.lykke.matching.engine.outgoing.messages

import com.fasterxml.jackson.annotation.JsonProperty
import com.lykke.matching.engine.daos.LimitOrder
import java.math.BigDecimal
import java.util.ArrayList
import java.util.Date
import java.util.concurrent.PriorityBlockingQueue

class OrderBook {
    val assetPair: String

    @get:JsonProperty("isBuy")
    @JsonProperty("isBuy")
    val isBuy: Boolean

    val timestamp: Date

    val prices: MutableList<Order> = ArrayList()

    val refMidPrice: BigDecimal?
    val refMidPricePeriod: Long

    constructor(assetPair: String,
                refMidPrice: BigDecimal,
                refMidPricePeriod: Long,
                isBuy: Boolean, timestamp: Date) {
        this.assetPair = assetPair
        this.isBuy = isBuy
        this.refMidPrice = refMidPrice
        this.refMidPricePeriod = refMidPricePeriod
        this.timestamp = timestamp
    }

    constructor(assetPair: String,
                refMidPrice: BigDecimal,
                refMidPricePeriod: Long,
                isBuy: Boolean,
                timestamp: Date,
                orders: PriorityBlockingQueue<LimitOrder>) {
        this.assetPair = assetPair
        this.refMidPrice = refMidPrice
        this.refMidPricePeriod = refMidPricePeriod
        this.isBuy = isBuy
        this.timestamp = timestamp

        while (!orders.isEmpty()) {
            val order = orders.poll()
            addVolumePrice(order.externalId, order.clientId, order.remainingVolume, order.price)
        }
    }

    fun addVolumePrice(id: String, clientId: String, volume: BigDecimal, price: BigDecimal) {
        prices.add(Order(id, clientId, volume, price))
    }
}

class Order(val id: String, val clientId: String, val volume: BigDecimal, val price: BigDecimal)
