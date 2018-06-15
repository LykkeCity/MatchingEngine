package com.lykke.matching.engine.daos

import java.math.BigDecimal
import java.util.Date

class BestPrice(val asset: String, val ask: BigDecimal, val bid: BigDecimal) {
    var dateTime = Date()

    override fun toString(): String {
        return "BestPrice(asset='$asset', ask=$ask, bid=$bid, dateTime=$dateTime)"
    }
}