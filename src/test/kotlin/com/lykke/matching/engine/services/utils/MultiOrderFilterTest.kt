package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.apache.log4j.Logger
import org.junit.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MultiOrderFilterTest {

    private val sortedOrders = listOf(buildLimitOrder(volume = -5.0, price = 1.1),
            buildLimitOrder(volume = -4.0, price = 1.2),
            buildLimitOrder(volume = -3.0, price = 1.3),
            buildLimitOrder(volume = -2.0, price = 1.4),
            buildLimitOrder(volume = -1.0, price = 1.5),
            buildLimitOrder(volume = 10.0, price = 1.0),
            buildLimitOrder(volume = 5.0, price = 0.9),
            buildLimitOrder(volume = 7.0, price = 0.75),
            buildLimitOrder(volume = 2.0, price = 0.7))

    private val notSortedOrders = listOf(sortedOrders[0],
            sortedOrders[1],
            sortedOrders[2],
            sortedOrders[4],
            sortedOrders[3],
            sortedOrders[8],
            sortedOrders[7],
            sortedOrders[6],
            sortedOrders[5])

    private fun createFilter(isTrustedClient: Boolean) = MultiOrderFilter(isTrustedClient,
            BigDecimal.valueOf(10.0),
            BigDecimal.valueOf(20.0),
            0,
            0,
            Logger.getLogger(MultiOrderFilterTest::class.java.name))

    @Test
    fun checkAndAdd() {
        var filter = createFilter(true)
        sortedOrders.forEach { filter.checkAndAdd(it) }
        assertEquals(6, filter.getResult().size)
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-5.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-4.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-1.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(10.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(5.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(2.0)) == 0 })

        filter = createFilter(false)
        sortedOrders.forEach { filter.checkAndAdd(it) }
        assertEquals(9, filter.getResult().size)

        filter = createFilter(true)
        notSortedOrders.forEach { filter.checkAndAdd(it) }
        assertEquals(8, filter.getResult().size)
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-5.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-4.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-2.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-1.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(10.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(7.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(5.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(2.0)) == 0 })

        filter = createFilter(false)
        notSortedOrders.forEach { filter.checkAndAdd(it) }
        assertEquals(9, filter.getResult().size)
    }

    @Test
    fun filterOutIfNotSorted() {
        var filter = createFilter(true)
        sortedOrders.forEach { filter.checkAndAdd(it) }
        assertTrue(filter.filterOutIfNotSorted().isEmpty())

        filter = createFilter(true)
        notSortedOrders.forEach { filter.checkAndAdd(it) }
        assertEquals(2, filter.filterOutIfNotSorted().size)
        assertEquals(6, filter.getResult().size)
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-5.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-4.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(-1.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(10.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(5.0)) == 0 })
        assertTrue(filter.getResult().any { it.volume.compareTo(BigDecimal.valueOf(2.0)) == 0 })
    }

}