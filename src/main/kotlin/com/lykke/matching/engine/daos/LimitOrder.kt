package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.Date

class LimitOrder: TableServiceEntity {

     var assetId: String = ""
     var clientId: String = ""
     var isOrderTaken: String = ""
     var orderType: String = ""
     var blockChain: String = ""
     var price: Double = 0.0
     var volume: Double = 0.0
     var status: String = ""
    //date from incoming message
     var createdAt: Date = Date()
    //date of registering by matching engine
     var registered: Date = Date()
    //date of execution
     var executed: Date? = null

    constructor() {}

    constructor(partitionKey: String, rowKey: String, assetId: String, clientId: String,
                executed: Date?, isOrderTaken: String, orderType: String, blockChain: String, price: Double,
                createdAt: Date, registered: Date, status: String, volume: Double) {
        this.partitionKey = partitionKey
        this.rowKey = rowKey
        this.assetId = assetId
        this.clientId = clientId
        this.executed = executed
        this.isOrderTaken = isOrderTaken
        this.orderType = orderType
        this.blockChain = blockChain
        this.price = price
        this.createdAt = createdAt
        this.registered = registered
        this.status = status
        this.volume = volume
    }
}