package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

data class VolumePrice(val volume: BigDecimal, val price: BigDecimal) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as VolumePrice

        if (!NumberUtils.equalsIgnoreScale(volume, other.volume)) return false
        if (!NumberUtils.equalsIgnoreScale(price, other.price)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = volume.stripTrailingZeros().hashCode()
        result = 31 * result + price.stripTrailingZeros().hashCode()
        return result
    }
}