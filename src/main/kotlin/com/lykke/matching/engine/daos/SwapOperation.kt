package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.Date

data class SwapOperation(val id: String, val externalId: String, val dateTime: Date,
                         val clientId1: String, val asset1: String, val volume1: BigDecimal,
                         val clientId2: String, val asset2: String, val volume2: BigDecimal) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SwapOperation

        if (id != other.id) return false
        if (externalId != other.externalId) return false
        if (dateTime != other.dateTime) return false
        if (clientId1 != other.clientId1) return false
        if (asset1 != other.asset1) return false
        if (!NumberUtils.equalsIgnoreScale(volume1, other.volume1)) return false
        if (clientId2 != other.clientId2) return false
        if (asset2 != other.asset2) return false
        if (!NumberUtils.equalsIgnoreScale(volume2, other.volume2)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + externalId.hashCode()
        result = 31 * result + dateTime.hashCode()
        result = 31 * result + clientId1.hashCode()
        result = 31 * result + asset1.hashCode()
        result = 31 * result + volume1.stripTrailingZeros().hashCode()
        result = 31 * result + clientId2.hashCode()
        result = 31 * result + asset2.hashCode()
        result = 31 * result + volume2.stripTrailingZeros().hashCode()
        return result
    }
}