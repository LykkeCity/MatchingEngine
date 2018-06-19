package com.lykke.matching.engine.notification

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

data class QuotesUpdate(val asset: String, val price: BigDecimal, val volume: BigDecimal) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as QuotesUpdate

        if (asset != other.asset) return false
        if (!NumberUtils.equalsIgnoreScale(price, other.price)) return false
        if (!NumberUtils.equalsIgnoreScale(volume, other.volume)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = asset.hashCode()
        result = 31 * result + price.stripTrailingZeros().hashCode()
        result = 31 * result + volume.stripTrailingZeros().hashCode()
        return result
    }
}