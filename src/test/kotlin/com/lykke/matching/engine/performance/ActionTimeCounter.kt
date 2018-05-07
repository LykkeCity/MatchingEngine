package com.lykke.matching.engine.performance

class ActionTimeCounter {
    var totalTime: Long = 0
    var totalCount: Long = 0

    fun executeAction(action: () -> Unit) {
        totalCount++

        val start = System.nanoTime()
        action.invoke()
        val end = System.nanoTime()

        totalTime += Math.abs(end - start)
    }

    fun getAverageTime(): Double {
        return 1.0 * totalTime / totalCount
    }
}