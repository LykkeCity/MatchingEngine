package com.lykke.matching.engine.daos

import java.io.Serializable
import java.util.Date
import java.util.LinkedList

open class Order: Serializable {
    var id: String = ""
    var externalId: String = ""
    var assetPairId: String = ""
    var clientId: String = ""
    var volume: Double = 0.0
    var status: String = ""
    var createdAt: Date = Date()
    var registered: Date = Date()

    constructor()
    constructor(id: String, externalId: String, assetPairId: String, clientId: String, volume: Double, status: String, createdAt: Date, registered: Date) {
        this.id = id
        this.externalId = externalId
        this.assetPairId = assetPairId
        this.clientId = clientId
        this.volume = volume
        this.status = status
        this.createdAt = createdAt
        this.registered = registered
    }


    var transactionIds: MutableList<String>? = null

    fun getAbsVolume(): Double {
        return Math.abs(volume)
    }


    open fun isBuySide(): Boolean {
        return volume > 0
    }

    fun addTransactionIds(ids: List<String>) {
        if (transactionIds == null) {
            transactionIds =  LinkedList()
        }

        transactionIds!!.addAll(ids)
    }
}