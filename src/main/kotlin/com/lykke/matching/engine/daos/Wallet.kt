package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class Wallet: TableServiceEntity {

    var balance: Double = 0.0

    constructor() {}

    constructor(clientId: String, asset: String, balance: Double = 0.0) {
        this.partitionKey = clientId
        this.rowKey = asset
        this.balance = balance
    }

    fun addBalance(amount: Double){
        balance += amount
    }

    fun getClientId(): String {
        return partitionKey
    }

    fun getAsset(): String {
        return rowKey
    }

    override fun toString(): String{
        return "Wallet(clientId=$partitionKey, asset=$rowKey, balance=$balance)"
    }
}