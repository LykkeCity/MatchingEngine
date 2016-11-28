package com.lykke.matching.engine.daos

import java.util.Date
import java.util.LinkedList

open class Order(var id: String, var uid: String, var assetPairId: String, var clientId: String,
                 var volume: Double, var status: String, var createdAt: Date,  var registered: Date) {

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