package com.lykke.matching.engine.services.utils

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.apache.log4j.Logger
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
        val sortedOrders = sortedOrders()
        return listOf(sortedOrders[0].copy(),
                sortedOrders[1].copy(),
                sortedOrders[2].copy(),
                sortedOrders[4].copy(),
                sortedOrders[3].copy(),
                sortedOrders[8].copy(),
                sortedOrders[7].copy(),
                sortedOrders[6].copy(),
                sortedOrders[5].copy())
    }

    private fun createFilter(isTrustedClient: Boolean) = MultiOrderFilter(isTrustedClient,
            BigDecimal.valueOf(10.0),
            BigDecimal.valueOf(20.0),
            0,
            Date(),
            0,
            Logger.getLogger(MultiOrderFilterTest::class.java.name))

    @Test
    fun checkAndAdd() {
        var filter = createFilter(true)
        sortedOrders().forEach { filter.checkAndAdd(it) }
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

        filter = createFilter(false)
        sortedOrders().forEach { filter.checkAndAdd(it) }
        assertEquals(9, filter.getResult().size)
        assertEquals(0, filter.getResult().filter { it.status == OrderStatus.NotEnoughFunds.name }.size)

        filter = createFilter(true)
        notSortedOrders().forEach { filter.checkAndAdd(it) }
        assertEquals(9, filter.getResult().size)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-5.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-4.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-1.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(10.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(5.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(2.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-2.0)) == 0 }.status)
        assertEquals(OrderStatus.NotEnoughFunds.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(-3.0)) == 0 }.status)
        assertEquals(OrderStatus.InOrderBook.name, filter.getResult().single { it.volume.compareTo(BigDecimal.valueOf(7.0)) == 0 }.status)

        filter = createFilter(false)
        notSortedOrders().forEach { filter.checkAndAdd(it) }
        assertEquals(9, filter.getResult().size)
        assertEquals(0, filter.getResult().filter { it.status == OrderStatus.NotEnoughFunds.name }.size)
    }

    @Test
    fun filterOutIfNotSorted() {
        var filter = createFilter(true)
        sortedOrders().forEach { filter.checkAndAdd(it) }
        val statuses1 = filter.getResult().map { it.status }
        filter.checkIfNotSorted()
        val statuses2 = filter.getResult().map { it.status }
        assertEquals(3, statuses2.filter { it == OrderStatus.NotEnoughFunds.name }.size)
        assertEquals(statuses1, statuses2)

        filter = createFilter(true)
        notSortedOrders().forEach { filter.checkAndAdd(it) }
        assertEquals(1, filter.getResult().filter { it.status == OrderStatus.NotEnoughFunds.name }.size)
        filter.checkIfNotSorted()
        assertEquals(3, filter.getResult().filter { it.status == OrderStatus.NotEnoughFunds.name }.size)
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