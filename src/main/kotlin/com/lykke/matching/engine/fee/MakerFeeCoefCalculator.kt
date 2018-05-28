package com.lykke.matching.engine.fee

import ch.obermuhlner.math.big.BigDecimalMath
import com.lykke.matching.engine.utils.RoundingUtils
import java.math.BigDecimal
import java.math.MathContext

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

        return BigDecimal.ONE - BigDecimalMath.exp(-spread * feeModificator, MathContext(RoundingUtils.MAX_SCALE_BIGDECIMAL_OPERATIONS))
    }
}