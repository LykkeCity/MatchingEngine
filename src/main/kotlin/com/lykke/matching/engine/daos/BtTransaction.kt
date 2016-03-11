package com.lykke.matching.engine.daos

import com.google.gson.Gson
import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.*

class BtTransaction : TableServiceEntity {

    var clientId: String?
    var created = Date()
    var requestData: String?

    constructor(partitionKey: String?, rowKey: String?, clientId: String? = null, created: Date, requestData: String, clientOrderPairs: List<ClientOrderPair>? = null) : super(partitionKey, rowKey) {
        this.clientId = clientId
        this.created = created
        this.requestData = requestData
        if (clientOrderPairs != null) {
            addClientOrdersIds(clientOrderPairs)
        }
    }

    fun loadClientOrderPairs(): MutableList<ClientOrderPair> {
        var result:MutableList<ClientOrderPair> = ArrayList()
        if (clientId != null) {
            result.addAll(Gson().fromJson(clientId, Array<ClientOrderPair>::class.java).asList())
        }

        return result
    }

    fun addClientOrdersIds(clientOrderPairs: List<ClientOrderPair>) {
        val orders = loadClientOrderPairs()
        orders.addAll(clientOrderPairs)
        saveClientOrdersIds(orders)
    }

    fun saveClientOrdersIds(clientOrderPairs: List<ClientOrderPair>) {
        this.clientId = Gson().toJson(clientOrderPairs)
    }
}

class ClientOrderPair(val ClientId: String, val OrderId: String)