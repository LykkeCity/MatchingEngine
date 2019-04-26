package com.lykke.matching.engine.daos.order

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

class MaxOrderVolumeInfo(private val maxValue: BigDecimal,
                         private val midPrice: BigDecimal) {

    val maxVolume: BigDecimal = NumberUtils.divideWithMaxScale(maxValue, midPrice)

    override fun toString(): String {
        return "maxValue=${NumberUtils.roundForPrint(maxValue)}, " +
                "midPrice=${NumberUtils.roundForPrint(midPrice)}, " +
                "maxVolume=${NumberUtils.roundForPrint(maxVolume)}"
    }
}