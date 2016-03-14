package com.lykke.matching.engine.daos

import com.google.gson.Gson
import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.ArrayList

class Wallet: TableServiceEntity {
    //partition key: "ClientBalance"
    //row key: clientId

    var balances: String? = null

    constructor() {}

    constructor(clientId: String, balances: String? = null) {
        this.partitionKey = "ClientBalance"
        this.rowKey = clientId
        this.balances = balances
    }

    fun addBalance(asset: String, amount: Double) {
        val assetBalances = getBalances()
        var assetBalance = assetBalances.find { it.Asset == asset }
        if (assetBalance == null) {
            assetBalance = AssetBalance(asset)
            assetBalances.add(assetBalance)
        }
        assetBalance.Balance += amount

        this.balances = Gson().toJson(assetBalances)
    }

    fun setBalance(asset: String, amount: Double) {
        val assetBalances = getBalances()
        var assetBalance = assetBalances.find { it.Asset == asset }
        if (assetBalance == null) {
            assetBalance = AssetBalance(asset)
            assetBalances.add(assetBalance)
        }

        assetBalance.Balance = amount

        this.balances = Gson().toJson(assetBalances)
    }

    fun getClientId(): String {
        return rowKey
    }

    fun getBalance(asset: String): Double {
        return getAssetBalance(asset).Balance
    }

    private fun getAssetBalance(asset: String): AssetBalance {
        if (balances != null) {
            val assetsBalances = getBalances()
            return assetsBalances.find { it.Asset == asset } ?: AssetBalance(asset, 0.0)
        }

        return AssetBalance(asset, 0.0)
    }

    fun getBalances(): MutableList<AssetBalance> {
        val result: MutableList<AssetBalance> = ArrayList()
        if (balances != null) {
            result.addAll(Gson().fromJson(balances, Array<AssetBalance>::class.java).asList())
        }
        return result
    }

    override fun toString(): String{
        return "Wallet(clientId=$rowKey, balances=$balances)"
    }
}

class AssetBalance(var Asset: String, var Balance: Double = 0.0)