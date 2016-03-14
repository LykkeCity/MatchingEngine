package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.Date

class BestPrice: TableServiceEntity {
    //partiotion key = "Feed"
    //rowkey = asset

    var ask = 0.0
    var bid = 0.0
    var dateTime = Date()

    constructor(asset: String, ask: Double, bid: Double): super("Feed", asset) {
        this.ask = ask
        this.bid = bid
    }
}