package com.lykke.matching.engine.fee

import java.math.BigDecimal
import java.math.BigInteger

class MakerFeeCoefCalculator(private val relativeSpread: BigDecimal?) : FeeCoefCalculator {

    var feeModificator: BigDecimal? = null

    override fun calculate(): BigDecimal? {
        val defaultValue = null
        val feeModificator = this.feeModificator ?: return defaultValue
        if (feeModificator <= BigDecimal.ZERO) {
            throw FeeException("makerFeeModificator should be greater than 0 (actual value: $feeModificator)")
        }
        val spread = relativeSpread ?: return defaultValue
        if (spread <= BigDecimal.ZERO) {
            throw FeeException("Spread should be greater than 0 (actual value: $spread)")
        }
        return BigDecimal.valueOf(1 - Math.exp(-spread.toDouble() * feeModificator.toDouble()))
    }
}