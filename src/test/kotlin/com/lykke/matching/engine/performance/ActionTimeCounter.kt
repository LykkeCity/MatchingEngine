package com.lykke.matching.engine.performance

class ActionTimeCounter {
    var totalTime: Long = 0
    var totalCount: Long = 0

    fun executeAction(action: () -> Unit) {
        totalCount++

        val start = System.currentTimeMillis()
        action.invoke()
        val end = System.currentTimeMillis()

        totalTime += end - start
    }

    fun getAverageTime(): Double {
        return 1.0 * totalTime / totalCount
    }
}