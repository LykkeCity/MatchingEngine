package com.lykke.matching.engine.daos

import java.time.Duration

enum class TickUpdateInterval(val prefix: String, val interval: Long){
    ONE_HOUR("1H", Duration.ofHours(1).toMillis()),
    ONE_DAY("1D", Duration.ofDays(1).toMillis()),
    THREE_DAYS("3D", Duration.ofDays(3).toMillis()),
    ONE_MONTH("1M", Duration.ofDays(30).toMillis()),
    ONE_YEAR("1Y", Duration.ofDays(365).toMillis());

    companion object {
        fun getByPrefix(prefix: String): TickUpdateInterval {
            TickUpdateInterval.values()
                    .filter { it.prefix == prefix }
                    .forEach { return it }
            throw IllegalArgumentException("TickUpdateInterval (prefix=$prefix) is not found")
        }
    }
}