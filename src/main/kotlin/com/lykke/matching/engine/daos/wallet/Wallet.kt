package com.lykke.matching.engine.daos.wallet

import java.io.Serializable
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

class Wallet: Serializable {
    val clientId: String
    val balances: MutableMap<String, AssetBalance> = ConcurrentHashMap()

    constructor(clientId: String) {
        this.clientId = clientId
    }

    constructor(clientId: String, balances: List<AssetBalance>) {
        this.clientId = clientId
        balances.forEach {
            this.balances[it.asset] = it
        }
    }

    fun setBalance(asset: String, timestamp: Date, balance: Double) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(asset, timestamp, balance)
        } else {
            oldBalance.balance = balance
        }
    }

    fun setReservedBalance(asset: String, timestamp: Date, reservedBalance: Double) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(asset, timestamp, reservedBalance, reservedBalance)
        } else {
            oldBalance.reserved = reservedBalance
        }
    }
}