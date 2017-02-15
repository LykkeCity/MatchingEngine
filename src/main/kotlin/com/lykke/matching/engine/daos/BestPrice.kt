package com.lykke.matching.engine.daos

import java.util.Date

class BestPrice(val asset: String, val ask: Double?, val bid: Double?) {
    var dateTime = Date()

    override fun toString(): String {
        return "BestPrice(asset='$asset', ask=$ask, bid=$bid, dateTime=$dateTime)"
    }
}