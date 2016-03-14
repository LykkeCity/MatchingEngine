package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class MatchingData: TableServiceEntity {
    //partition key: master order id
    //row key: matched order id

    var volume: Double = 0.0

    constructor(masterOrderId: String, matchedOrderId: String, volume: Double) : super(masterOrderId, matchedOrderId) {
        this.volume = volume
    }
}