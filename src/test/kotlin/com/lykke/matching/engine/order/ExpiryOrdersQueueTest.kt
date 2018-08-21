package com.lykke.matching.engine.order

import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Test
import java.text.SimpleDateFormat
import java.util.Date
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ExpiryOrdersQueueTest {


    private val dateFormat = SimpleDateFormat("yyyy-MM-dd hh:mm")
    private val queue = ExpiryOrdersQueue()

    @Test
    fun addOrder() {
        assertFalse(queue.addOrder(buildLimitOrder()))
        assertFalse(queue.addOrder(buildLimitOrder(expiryTime = Date())))
        assertFalse(queue.addOrder(buildLimitOrder(timeInForce = OrderTimeInForce.GTC, expiryTime = Date())))
        assertTrue(queue.addOrder(buildLimitOrder(timeInForce = OrderTimeInForce.GTD, expiryTime = Date())))
    }

    @Test
    fun getExpiredOrders() {
        queue.addOrder(buildLimitOrder())
        queue.addOrder(buildLimitOrder(expiryTime = date("2018-03-20 10:10")))
        queue.addOrder(buildLimitOrder(timeInForce = OrderTimeInForce.GTC, expiryTime = date("2018-03-20 10:10")))

        queue.addOrder(buildLimitOrder(uid = "1", timeInForce = OrderTimeInForce.GTD, expiryTime = date("2018-03-20 10:10")))
        queue.addOrder(buildLimitOrder(uid = "2", timeInForce = OrderTimeInForce.GTD, expiryTime = date("2018-03-20 20:10")))
        queue.addOrder(buildLimitOrder(uid = "3", timeInForce = OrderTimeInForce.GTD, expiryTime = date("2018-03-20 20:10"), registered = date("2018-03-19 20:10")))
        queue.addOrder(buildLimitOrder(uid = "4", timeInForce = OrderTimeInForce.GTD, expiryTime = date("2018-03-21 10:10")))

        assertTrue(queue.getExpiredOrderExternalIds(date("2018-03-20 10:00")).isEmpty())
        assertEquals(listOf("1"), queue.getExpiredOrderExternalIds(date("2018-03-20 10:10")))
        assertEquals(listOf("1"), queue.getExpiredOrderExternalIds(date("2018-03-20 10:11")))
        assertEquals(listOf("1"), queue.getExpiredOrderExternalIds(date("2018-03-20 20:09")))
        assertEquals(listOf("1", "3", "2"), queue.getExpiredOrderExternalIds(date("2018-03-20 20:11")))
        assertEquals(listOf("1", "3", "2", "4"), queue.getExpiredOrderExternalIds(date("2018-03-22 00:00")))
    }

    @Test
    fun removeOrder() {
        val order = buildLimitOrder(timeInForce = OrderTimeInForce.GTD, expiryTime = date("2018-03-20 10:10"))
        queue.addOrder(order)
        assertEquals(listOf(order.externalId), queue.getExpiredOrderExternalIds(date("2018-03-20 10:11")))
        assertTrue(queue.removeOrder(order))
        assertTrue(queue.getExpiredOrderExternalIds(date("2018-03-20 10:11")).isEmpty())
    }

    private fun date(date: String) = dateFormat.parse(date)
}