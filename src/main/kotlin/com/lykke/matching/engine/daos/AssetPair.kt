package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class AssetPair : TableServiceEntity {

    var baseAssetId = ""
    var quotingAssetId = ""

    constructor() {}

    constructor(baseAssetId: String, quotingAssetId: String) : super("AssetPair", "$baseAssetId$quotingAssetId") {
        this.baseAssetId = baseAssetId
        this.quotingAssetId = quotingAssetId
    }

    fun getAssetPairId(): String {
        return rowKey
    }
}