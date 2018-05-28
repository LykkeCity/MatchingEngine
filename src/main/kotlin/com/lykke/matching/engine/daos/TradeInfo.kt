package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.RoundingUtils
import java.math.BigDecimal
import java.util.Date

data class TradeInfo(val assetPair: String, val isBuy: Boolean, val price: BigDecimal, val date: Date) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TradeInfo

        if (assetPair != other.assetPair) return false
        if (isBuy != other.isBuy) return false
        if (!RoundingUtils.equalsIgnoreScale(price, other.price)) return false
        if (date != other.date) return false

        return true
    }

    override fun hashCode(): Int {
        var result = assetPair.hashCode()
        result = 31 * result + isBuy.hashCode()
        result = 31 * result + price.stripTrailingZeros().hashCode()
        result = 31 * result + date.hashCode()
        return result
    }
}