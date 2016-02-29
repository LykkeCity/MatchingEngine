package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.Date

class WalletOperation: TableServiceEntity {
    //partition key: Client Id
    //row key: uid


    var dateTime: Date = Date()
    var assetId: String = ""
    var amount: Double = 0.0

    constructor() {}

    constructor(clientId: String, uid: String, dateTime: Date, asset: String, amount: Double) {
        this.partitionKey = clientId
        this.rowKey = uid
        this.dateTime = dateTime
        this.assetId = asset
        this.amount = amount
    }

    fun getClientId(): String {
        return partitionKey
    }

    fun getUid(): String {
        return rowKey
    }

    override fun toString(): String{
        return "WalletOperation(clientId=$partitionKey, uid=$rowKey, dateTime=$dateTime, assetId='$assetId', amount=$amount)"
    }
}