package com.lykke.matching.engine.utils

import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.junit.Test
import java.math.BigDecimal
import kotlin.test.assertEquals

class RoundingUtilsTest {

    @Test
    fun testRound() {
        assertEquals(0.03154761, RoundingUtils.round(0.032 - 0.00045239, 8, true))
        assertEquals(91.1, RoundingUtils.round(91.1, 4, false))
        assertEquals(-0.01, RoundingUtils.round(-1.0 * 0.00000001, 2, true))
        assertEquals(18.57, RoundingUtils.round(0.69031943 * 26.915076, 2, false))
    }

    @Test
    fun testEqualsWithDefaultDelta() {
        assertTrue(RoundingUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.01), BigDecimal.valueOf(0.01)))
        assertTrue(RoundingUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.10000000001), BigDecimal.valueOf(0.1)))
        assertFalse(RoundingUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.1000001), BigDecimal.valueOf(0.1)))
    }

}