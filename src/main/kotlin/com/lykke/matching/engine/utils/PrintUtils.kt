package com.lykke.matching.engine.utils

class PrintUtils {
    companion object {
        fun convertToString(value: Double): String {
            if ((value / 100000).toInt() == 0) {
                //microseconds
                return "${RoundingUtils.roundForPrint(value / 1000)} micros"
            } else {
                //milliseconds
                return "${RoundingUtils.roundForPrint(value / 1000000)} millis"
            }
        }
    }
}