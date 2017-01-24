package com.lykke.matching.engine

import com.lykke.matching.engine.utils.RoundingUtils

val PRECISION = 0.0000000001
fun Double.greaterThan(other: Double): Boolean {
    return Math.abs(this - other) > PRECISION
}

fun Double.round(accuracy: Int): String {
    return RoundingUtils.parseDouble(this, accuracy).toPlainString()
}
