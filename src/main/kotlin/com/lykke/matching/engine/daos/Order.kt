package com.lykke.matching.engine.daos

import java.io.Serializable
import java.util.Date

abstract class Order: Serializable {
    var id: String = ""
    var externalId: String = ""
    var assetPairId: String = ""
    var clientId: String = ""
    var volume: Double = 0.0
    var status: String = ""
    var createdAt: Date = Date()
    var registered: Date = Date()
    var dustSize: Double? = 0.0

    constructor()
    constructor(id: String, externalId: String, assetPairId: String, clientId: String, volume: Double, status: String, createdAt: Date, registered: Date, dustSize: Double?) {
        this.id = id
        this.externalId = externalId
        this.assetPairId = assetPairId
        this.clientId = clientId
        this.volume = volume
        this.status = status
        this.createdAt = createdAt
        this.registered = registered
        this.dustSize = dustSize
    }

    fun getAbsVolume(): Double {
        return Math.abs(volume)
    }

    open fun isBuySide(): Boolean {
        return volume > 0
    }

    abstract fun isOrigBuySide(): Boolean
    abstract fun isStraight(): Boolean
    abstract fun calculateReservedVolume(): Double
    abstract fun updateMatchTime(time: Date)
    abstract fun takePrice(): Double?
    abstract fun updatePrice(price: Double)
    abstract fun updateRemainingVolume(price: Double)
}