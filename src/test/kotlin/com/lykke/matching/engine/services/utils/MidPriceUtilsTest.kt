package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.utils.assertEquals
import junit.framework.Assert.assertNull
import org.junit.Test
import java.math.BigDecimal

class MidPriceUtilsTest {

    @Test
    fun getLULDBoundsTest() {
        val (lowerBound, upperBound) = MidPriceUtils.getMidPricesInterval(BigDecimal.valueOf(0.01), BigDecimal.valueOf(100))
        assertEquals(BigDecimal.valueOf(99.0), lowerBound)
        assertEquals(BigDecimal.valueOf(101.0), upperBound)
    }

    @Test
    fun getLULDBoundsPassingNoThreshold() {
        val (lowerBound, upperBound) = MidPriceUtils.getMidPricesInterval(null, BigDecimal.ZERO)
        assertNull(lowerBound)
        assertNull(upperBound)
    }
}