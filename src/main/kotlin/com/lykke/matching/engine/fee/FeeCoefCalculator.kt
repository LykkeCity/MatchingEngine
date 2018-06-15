package com.lykke.matching.engine.fee

import java.math.BigDecimal

interface FeeCoefCalculator {
    fun calculate(): BigDecimal?
}