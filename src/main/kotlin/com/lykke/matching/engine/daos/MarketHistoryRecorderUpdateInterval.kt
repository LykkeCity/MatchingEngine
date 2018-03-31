package com.lykke.matching.engine.daos

import java.time.Duration
import java.time.temporal.ChronoUnit

enum class MarketHistoryRecorderUpdateInterval(val prefix: String, val period: Long){
    ONE_HOUR("1H", Duration.ofHours(1).get(ChronoUnit.MILLIS)),
    ONE_DAY("1D", Duration.ofDays(1).get(ChronoUnit.MILLIS)),
    THREE_DAYS("3D", Duration.ofDays(3).get(ChronoUnit.MILLIS)),
    ONE_MONTH("1M", Duration.ofDays(30).get(ChronoUnit.MILLIS)),
    ONE_YEAR("1Y", Duration.ofDays(365).get(ChronoUnit.MILLIS));

    companion object {
        fun getByPrefix(prefix: String): MarketHistoryRecorderUpdateInterval {
            MarketHistoryRecorderUpdateInterval.values()
                    .filter { it.prefix == prefix }
                    .forEach { return it }
            throw IllegalArgumentException("MarketHistoryRecorderUpdateInterval (prefix=$prefix) is not found")
        }
    }
}