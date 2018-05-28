package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.RoundingUtils
import java.math.BigDecimal

data class MatchingData(val masterOrderId: String, val matchedOrderId: String, val volume: BigDecimal) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MatchingData

        if (masterOrderId != other.masterOrderId) return false
        if (matchedOrderId != other.matchedOrderId) return false
        if (!RoundingUtils.equalsIgnoreScale(volume, other.volume)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = masterOrderId.hashCode()
        result = 31 * result + matchedOrderId.hashCode()
        result = 31 * result + volume.stripTrailingZeros().hashCode()
        return result
    }
}