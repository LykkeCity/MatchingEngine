package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class Candle: TableServiceEntity {
    var data: String? = null

    constructor(partitionKey: String, rowKey: String, data: String): super(partitionKey, rowKey) {
        this.data = data
    }
}