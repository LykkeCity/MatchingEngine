package com.lykke.matching.engine.performance


class Runner {
    companion object {
        fun runTests(times: Int, vararg testCases:  () -> Double): Double {
            var totalTime = 0.0
            testCases.forEach {
                val testCase = it
                repeat(times) {totalTime += testCase.invoke()}
            }

            return totalTime / (times * 1.0 * testCases.size)
        }
    }
}