package com.lykke.matching.engine

import com.lykke.matching.engine.utils.NumberUtils

val PRECISION = 0.0000000001
fun Double.greaterThan(other: Double): Boolean {
    return Math.abs(this - other) > PRECISION
}

fun Double.round(accuracy: Int): String {
    return NumberUtils.parseDouble(this, accuracy).toPlainString()
}
