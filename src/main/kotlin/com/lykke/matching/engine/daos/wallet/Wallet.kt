package com.lykke.matching.engine.daos.wallet

import java.util.HashMap

class Wallet {
    val clientId: String
    val balances: MutableMap<String, AssetBalance> = HashMap()

    constructor(clientId: String) {
        this.clientId = clientId
    }

    constructor(clientId: String, balances: List<AssetBalance>) {
        this.clientId = clientId
        balances.forEach {
            this.balances[it.asset] = it
        }
    }

    fun setBalance(asset: String, balance: Double) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(asset, balance)
        } else {
            oldBalance.balance = balance
        }
    }

    fun setReservedBalance(asset: String, reservedBalance: Double) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(asset, reservedBalance, reservedBalance)
        } else {
            oldBalance.reserved = reservedBalance
        }
    }
}