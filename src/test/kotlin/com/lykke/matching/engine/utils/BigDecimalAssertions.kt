package com.lykke.matching.engine.utils

import java.math.BigDecimal

fun assertEquals(expected: BigDecimal, actual: BigDecimal) {
    if (!RoundingUtils.equalsIgnoreScale(expected, actual)) {
        kotlin.test.assertEquals(expected, actual)
    }
}