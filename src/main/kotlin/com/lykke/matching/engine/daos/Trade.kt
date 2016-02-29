package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.Date

class Trade: TableServiceEntity {
    //partition key: Client id
    //row key: generated uid

    var assetId: String = ""
    var dateTime: Date = Date()
    var limitOrderId: String = ""
    var marketOrderId: String = ""
    var volume: Double = 0.0

    constructor() : super()

    constructor(partitionKey: String, rowKey: String, assetId: String, dateTime: Date, limitOrderId: String, 
                marketOrderId: String,  volume: Double) : super(partitionKey, rowKey) {
        this.assetId = assetId
        this.dateTime = dateTime
        this.limitOrderId = limitOrderId
        this.marketOrderId = marketOrderId
        this.volume = volume
    }

    fun getClientId() = partitionKey

    override fun toString(): String{
        return "Trade(clientId=$partitionKey, uid=$rowKey, assetId='$assetId', dateTime=$dateTime, limitOrderId='$limitOrderId', marketOrderId='$marketOrderId', volume=$volume)"
    }
}