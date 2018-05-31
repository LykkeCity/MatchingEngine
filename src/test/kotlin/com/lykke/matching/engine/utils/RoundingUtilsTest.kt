package com.lykke.matching.engine.utils

import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.junit.Test
import java.math.BigDecimal

class RoundingUtilsTest {

    @Test
    fun testEqualsWithDefaultDelta() {
        assertTrue(NumberUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.01), BigDecimal.valueOf(0.01)))
        assertTrue(NumberUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.10000000001), BigDecimal.valueOf(0.1)))
        assertFalse(NumberUtils.equalsWithDefaultDelta(BigDecimal.valueOf(0.1000001), BigDecimal.valueOf(0.1)))
    }
}