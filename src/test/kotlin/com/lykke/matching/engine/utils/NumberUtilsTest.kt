package com.lykke.matching.engine.utils

import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.junit.Test
import kotlin.test.assertEquals

class NumberUtilsTest {

    @Test
    fun testRound() {
        assertEquals(0.03154761, NumberUtils.round(0.032 - 0.00045239, 8, true))
        assertEquals(91.1, NumberUtils.round(91.1, 4, false))
        assertEquals(-0.01, NumberUtils.round(-1.0 * 0.00000001, 2, true))
        assertEquals(18.57, NumberUtils.round(0.69031943 * 26.915076, 2, false))
    }

    @Test
    fun testScaleCheck() {
        assertTrue(NumberUtils.isScaleSmallerOrEqual(0.033, 3))
        assertTrue(NumberUtils.isScaleSmallerOrEqual(0.0330, 3))
        assertTrue(NumberUtils.isScaleSmallerOrEqual(0.033, 4))
        assertTrue(NumberUtils.isScaleSmallerOrEqual(1.0, 4))

        assertFalse(NumberUtils.isScaleSmallerOrEqual(0.033, 2))
    }
}