package com.lykke.matching.engine.utils

import org.junit.Test
import kotlin.test.assertEquals

class RoundingUtilsTest {

    @Test
    fun testRound() {
        assertEquals(0.03154761, RoundingUtils.round(0.032 - 0.00045239, 8, true))
        assertEquals(91.1, RoundingUtils.round(91.1, 4, false))
        assertEquals(-0.01, RoundingUtils.round(-1.0 * 0.00000001, 2, true))
        assertEquals(18.57, RoundingUtils.round(0.69031943 * 26.915076, 2, false))
    }

}