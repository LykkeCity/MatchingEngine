package com.lykke.matching.engine.utils

class PrintUtils {
    companion object {
        fun convertToString(value: Double): String {
            if ((value / 100000).toInt() == 0) {
                //microseconds
                return "${NumberUtils.roundForPrint(value / 1000)} micros"
            } else {
                //milliseconds
                return "${NumberUtils.roundForPrint(value / 1000000)} millis"
            }
        }

        fun convertToString2(value: Double): String {
            if ((value / 100000).toInt() == 0) {
                //microseconds
                return "${NumberUtils.roundForPrint2(value / 1000)} micros"
            } else {
                //milliseconds
                return "${NumberUtils.roundForPrint2(value / 1000000)} millis"
            }
        }
    }
}