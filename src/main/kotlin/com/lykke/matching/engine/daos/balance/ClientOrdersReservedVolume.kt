package com.lykke.matching.engine.daos.balance

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.LinkedList

data class ClientOrdersReservedVolume(var volume: BigDecimal = BigDecimal.ZERO, val orderIds: MutableList<String> = LinkedList()) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ClientOrdersReservedVolume

        if (!NumberUtils.equalsIgnoreScale(volume, other.volume)) return false
        if (orderIds != other.orderIds) return false

        return true
    }

    override fun hashCode(): Int {
        var result = volume.stripTrailingZeros().hashCode()
        result = 31 * result + orderIds.hashCode()
        return result
    }
}