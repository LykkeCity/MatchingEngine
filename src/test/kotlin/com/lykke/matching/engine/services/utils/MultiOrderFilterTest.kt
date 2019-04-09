package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.slf4j.LoggerFactory
import org.junit.Test
import java.math.BigDecimal
import java.util.Date
import kotlin.test.assertEquals

class MultiOrderFilterTest {

    private fun sortedOrders() = listOf(buildLimitOrder(volume = -5.0, price = 1.1),
            buildLimitOrder(volume = -4.0, price = 1.2),
            buildLimitOrder(volume = -3.0, price = 1.3),
            buildLimitOrder(volume = -2.0, price = 1.4),
            buildLimitOrder(volume = -1.0, price = 1.5),
            buildLimitOrder(volume = 10.0, price = 1.0),
            buildLimitOrder(volume = 5.0, price = 0.9),
            buildLimitOrder(volume = 7.0, price = 0.75),
            buildLimitOrder(volume = 2.0, price = 0.7))

    private fun notSortedOrders(): List<LimitOrder> {
        return sortedOrders().map { it.copy() }.shuffled()
    }

    private fun createFilter(isTrustedClient: Boolean) = MultiOrderFilter(isTrustedClient,
            BigDecimal.valueOf(10.0),
            BigDecimal.valueOf(20.0),
            0,
            Date(),
            0,
            LoggerFactory.getLogger(MultiOrderFilterTest::class.java.name))

    @Test
    fun testSortedOrdersOfTrustedClient() {
        val filter = createFilter(true)
        sortedOrders().forEach { filter.checkAndAdd(it) }
        assertFilteredResult(filter)
    }

    @Test
    fun testNotSortedOrdersOfTrustedClient() {
        // 1000 cases to test different shuffled orders
        (1..1000).forEach {
            val filter = createFilter(true)
            notSortedOrders().forEach { filter.checkAndAdd(it) }
            assertFilteredResult(filter)
        }
    }

    @Test
    fun testSortedOrdersOfNotTrustedClient() {
        val filter = createFilter(false)
        sortedOrders().forEach { filter.checkAndAdd(it) }
        assertEquals(9, filter.getResult().size)
        assertEquals(0, filter.getResult().filter { it.status == OrderStatus.NotEnoughFunds.name }.size)
    }

    @Test
    fun testNotSortedOrdersOfNotTrustedClient() {
        val filter = createFilter(false)
        notSortedOrders().forEach { filter.checkAndAdd(it) }
        assertEquals(9, filter.getResult().size)
        assertEquals(0, filter.getResult().filter { it.status == OrderStatus.NotEnoughFunds.name }.size)
    }

    private fun assertFilteredResult(filter: MultiOrderFilter) {
        assertEquals(9, filter.getResult().size)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-5.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-4.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-1.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(10.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(5.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(2.0)) == 0 }.status)
        assertEquals(OrderStatus.NotEnoughFunds.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-2.0)) == 0 }.status)
        assertEquals(OrderStatus.NotEnoughFunds.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-3.0)) == 0 }.status)
        assertEquals(OrderStatus.NotEnoughFunds.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(7.0)) == 0 }.status)
    }
}