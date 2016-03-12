package com.lykke.matching.engine.daos

import com.google.gson.Gson
import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.*

class BtTransaction : TableServiceEntity {

    var contextData: String? = null
    var created = Date()
    var requestData: String?

    constructor(partitionKey: String?, rowKey: String?, created: Date, requestData: String,
                clientCashOperationPair: ClientCashOperationPair? = null, orders: Orders? = null) : super(partitionKey, rowKey) {
        this.created = created
        this.requestData = requestData
        if (clientCashOperationPair != null) {
            addClientCashOperationPair(clientCashOperationPair)
        }
        if (orders != null) {
            addOrders(orders)
        }
    }

    fun addOrders(orders: Orders) {
        this.contextData = Gson().toJson(orders)
    }

    fun addClientCashOperationPair(clientCashOperationPair: ClientCashOperationPair) {
        this.contextData = Gson().toJson(clientCashOperationPair)
    }
}

class ClientOrderPair(val ClientId: String, val OrderId: String)
class Orders(val MarketOrder: ClientOrderPair, val ClientOrder: ClientOrderPair)
class ClientCashOperationPair(val ClientId: String, val CashOperationId: String)