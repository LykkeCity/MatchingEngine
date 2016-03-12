package com.lykke.matching.engine.daos

import com.google.gson.Gson
import com.lykke.matching.engine.order.OrderSide
import com.lykke.matching.engine.order.OrderSide.Buy
import com.lykke.matching.engine.order.OrderSide.Sell
import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.*

abstract class Order: TableServiceEntity {
    //partition key: client_id
    //row key: uid

    var assetPairId: String = ""
    var clientId: String = ""
    // volume > 0 - Buy side, otherwise - Sell side
    var volume: Double = 0.0
    var status: String = ""
    //date from incoming message
    var createdAt: Date = Date()
    //date of registering by matching engine
    var registered: Date = Date()
    var transactionId: String? = null

    constructor() {}

    constructor(uid: String, assetPairId: String, clientId: String, createdAt: Date, registered: Date,
                status: String, volume: Double, transactionId: String? = null) : super(clientId, uid) {
        this.assetPairId = assetPairId
        this.clientId = clientId
        this.createdAt = createdAt
        this.registered = registered
        this.status = status
        this.volume = volume
        this.transactionId = transactionId
    }
    fun getId() = rowKey

    fun addTransactionIds(ids: List<String>) {
        val transactions = loadTransactionsId()
        transactions.addAll(ids)
        saveTransactionsId(transactions)
    }

    fun loadTransactionsId(): MutableList<String> {
        var result:MutableList<String> = ArrayList()
        if (transactionId != null) {
            result.addAll(Gson().fromJson(transactionId, Array<String>::class.java).asList())
        }

        return result
    }

    fun saveTransactionsId(orders: List<String>) {
        this.transactionId = Gson().toJson(orders)
    }

    fun isBuySide(): Boolean {
        return isBuySide(volume)
    }

    fun getSide(): OrderSide {
        return getSide(volume)
    }

    fun getAbsVolume(): Double {
        return Math.abs(volume)
    }

    companion object {
        fun buildPartitionKey(assetPairId: String, side: OrderSide): String {
            return "${assetPairId}_${side.name}"
        }

        fun isBuySide(volume: Double): Boolean {
            return volume > 0
        }

        fun getSide(volume: Double): OrderSide {
            return if (isBuySide(volume)) Buy else Sell
        }
    }
}