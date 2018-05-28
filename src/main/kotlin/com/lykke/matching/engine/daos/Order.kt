package com.lykke.matching.engine.daos

import java.io.Serializable
import java.math.BigDecimal
import java.util.Date

open class Order: Serializable {
    var id: String = ""
    var externalId: String = ""
    var assetPairId: String = ""
    var clientId: String = ""
    var volume: BigDecimal = BigDecimal.ZERO
    var status: String = ""
    var createdAt: Date = Date()
    var registered: Date = Date()

    constructor()
    constructor(id: String, externalId: String, assetPairId: String, clientId: String,
                volume: BigDecimal, status: String, createdAt: Date, registered: Date) {
        this.id = id
        this.externalId = externalId
        this.assetPairId = assetPairId
        this.clientId = clientId
        this.volume = volume
        this.status = status
        this.createdAt = createdAt
        this.registered = registered
    }

    fun getAbsVolume(): BigDecimal {
        return volume.abs()
    }

    open fun isBuySide(): Boolean {
        return volume > BigDecimal.ZERO
    }
}