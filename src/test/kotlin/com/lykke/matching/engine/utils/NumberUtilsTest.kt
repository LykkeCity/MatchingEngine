package com.lykke.matching.engine.utils

import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.junit.Test
import java.math.BigDecimal

class NumberUtilsTest {


    @Test
    fun testScaleCheck() {
        assertTrue(NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(0.033), 3))
        assertTrue(NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(0.0330), 3))
        assertTrue(NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(0.033), 4))
        assertTrue(NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(1.0), 4))

        assertFalse(NumberUtils.isScaleSmallerOrEqual(BigDecimal.valueOf(0.033), 2))
    }

    @Test
    fun testEqualsWithDefaultDelta() {
        assertTrue(NumberUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.01), BigDecimal.valueOf(0.01)))
        assertTrue(NumberUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.10000000001), BigDecimal.valueOf(0.1)))
        assertFalse(NumberUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.1000001), BigDecimal.valueOf(0.1)))
    }
}