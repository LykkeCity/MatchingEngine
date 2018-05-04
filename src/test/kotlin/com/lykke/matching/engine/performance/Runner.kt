package com.lykke.matching.engine.performance


class Runner {
    companion object {
        fun runTests(times: Int, vararg testCases:  () -> Unit): Double {
            val startTime = System.currentTimeMillis()
            testCases.forEach {
                val testCase = it
                repeat(times) {testCase.invoke()}
            }
            val endTime = System.currentTimeMillis()

            return (endTime - startTime) / (times * 1.0 * testCases.size)
        }
    }
}