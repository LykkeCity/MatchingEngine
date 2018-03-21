package com.lykke.matching.engine.fee

class MakerFeeCoefCalculator(private val relativeSpread: Double?) : FeeCoefCalculator {

    var feeModificator: Double? = null

    override fun calculate(): Double? {
        val defaultValue = null
        val feeModificator = this.feeModificator ?: return defaultValue
        if (feeModificator <= 0.0) {
            throw FeeException("makerFeeModificator should be greater than 0 (actual value: $feeModificator)")
        }
        val spread = relativeSpread ?: return defaultValue
        if (spread <= 0.0) {
            throw FeeException("Spread should be greater than 0 (actual value: $spread)")
        }
        return 1 - Math.exp(-spread * feeModificator)
    }
}