package com.lykke.matching.engine.daos.wallet

import com.lykke.matching.engine.exception.BalanceException
import com.lykke.matching.engine.updater.Copyable
import java.util.HashMap

class Wallet: Copyable {

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

    fun validate() {
        balances.forEach { _, assetBalance ->
            try {
                assetBalance.validate()
            } catch (e: BalanceException) {
                throw BalanceException("Invalid wallet for client=$clientId: ${e.message}")
            }
        }
    }

    override fun copy(): Wallet {
        return Wallet(clientId, balances.values.map { it.copy() }.toList())
    }

    override fun applyToOrigin(origin: Copyable) {
        origin as Wallet
        val originBalances = origin.balances
        originBalances.putAll(balances.mapValues {
            val assetPairId = it.key
            val assetBalance = it.value
            if (originBalances.containsKey(assetPairId)) {
                val originAssetBalance = originBalances[assetPairId]!!
                assetBalance.applyToOrigin(originAssetBalance)
                originAssetBalance
            } else {
                assetBalance
            }
        })
    }
}