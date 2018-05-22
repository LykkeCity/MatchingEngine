package com.lykke.matching.engine.utils

import java.math.BigDecimal

fun assertEquals(expected: BigDecimal?, actual: BigDecimal?) {
    if (expected == null && actual == null)  {
        return
    }

    if (!RoundingUtils.equalsIgnoreScale(expected, actual)) {
        kotlin.test.assertEquals(expected, actual)
    }
}