package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class AssetPair : TableServiceEntity {

    var baseAssetId:String? = null
    var quotingAssetId:String?  = null

    constructor() {}

    constructor(baseAssetId: String, quotingAssetId: String) : super("AssetPair", "$baseAssetId$quotingAssetId") {
        this.baseAssetId = baseAssetId
        this.quotingAssetId = quotingAssetId
    }

    fun getAssetPairId(): String {
        return rowKey
    }

    override fun toString(): String{
        return "AssetPair(baseAssetId=$baseAssetId, quotingAssetId=$quotingAssetId)"
    }
}