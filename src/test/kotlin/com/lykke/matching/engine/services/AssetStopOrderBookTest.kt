package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Test
import org.junit.Before
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class AssetStopOrderBookTest {

    private lateinit var orderBook: AssetStopOrderBook

    @Before
    fun setUp() {
        orderBook = AssetStopOrderBook("AssetPair")

        // Ask stop orders
        orderBook.addOrder(buildLimitOrder(
                uid = "order1", assetId = "AssetPair", volume = -1.0, type = LimitOrderType.STOP_LIMIT,
                lowerLimitPrice = 1.0, lowerPrice = 0.95, upperLimitPrice = 1.15, upperPrice = 1.1
        ))
        orderBook.addOrder(buildLimitOrder(
                uid = "order2", assetId = "AssetPair", volume = -1.0, type = LimitOrderType.STOP_LIMIT,
                lowerLimitPrice = 0.99, lowerPrice = 0.94, upperLimitPrice = 1.14, upperPrice = 1.11
        ))

        // Bid stop orders
        orderBook.addOrder(buildLimitOrder(
                uid = "order3", assetId = "AssetPair", volume = 1.0, type = LimitOrderType.STOP_LIMIT,
                lowerLimitPrice = 1.15, lowerPrice = 1.2, upperLimitPrice = 1.3, upperPrice = 1.4
        ))
        orderBook.addOrder(buildLimitOrder(
                uid = "order4", assetId = "AssetPair", volume = 1.0, type = LimitOrderType.STOP_LIMIT,
                lowerLimitPrice = 1.14, lowerPrice = 1.21, upperLimitPrice = 1.25, upperPrice = 1.35
        ))

    }

    @Test
    fun getBidOrder() {
        assertNull(orderBook.getOrder(BigDecimal.valueOf( 1.2), true, true))
        assertNull(orderBook.getOrder(BigDecimal.valueOf(1.2), true, false))

        assertNull(orderBook.getOrder(BigDecimal.valueOf(1.13), true, false))
        var order = orderBook.getOrder(BigDecimal.valueOf(1.13), true, true)
        assertNotNull(order)
        assertEquals("order3", order!!.externalId)

        assertNull(orderBook.getOrder(BigDecimal.valueOf(1.31), true, true))
        order = orderBook.getOrder(BigDecimal.valueOf(1.31), true, false)
        assertNotNull(order)
        assertEquals("order4", order!!.externalId)

        // the same limit price and the best own price - in this case the earlier order must be returned
        orderBook.addOrder(buildLimitOrder(
                uid = "order5", assetId = "AssetPair", volume = 1.0, type = LimitOrderType.STOP_LIMIT,
                lowerLimitPrice = 1.15, lowerPrice = 1.21, upperLimitPrice = 1.25, upperPrice = 1.4
        ))

        order = orderBook.getOrder(BigDecimal.valueOf(1.15), true, true)
        assertNotNull(order)
        assertEquals("order3", order!!.externalId)

        order = orderBook.getOrder(BigDecimal.valueOf(1.25), true, false)
        assertNotNull(order)
        assertEquals("order4", order!!.externalId)
    }

    @Test
    fun getAskOrder() {
        assertNull(orderBook.getOrder(BigDecimal.valueOf(1.1), false, true))
        assertNull(orderBook.getOrder(BigDecimal.valueOf(1.1), false, false))

        assertNull(orderBook.getOrder(BigDecimal.valueOf(0.98), false, false))
        var order = orderBook.getOrder(BigDecimal.valueOf(0.98), false, true)
        assertNotNull(order)
        assertEquals("order1", order!!.externalId)

        assertNull(orderBook.getOrder(BigDecimal.valueOf(1.16), false, true))
        order = orderBook.getOrder(BigDecimal.valueOf(1.16), false, false)
        assertNotNull(order)
        assertEquals("order2", order!!.externalId)

        // the same limit price and the best own price - in this case the earlier order must be returned
        orderBook.addOrder(buildLimitOrder(
                uid = "order5", assetId = "AssetPair", volume = -1.0, type = LimitOrderType.STOP_LIMIT,
                lowerLimitPrice = 1.0, lowerPrice = 0.94, upperLimitPrice = 1.14, upperPrice = 1.1
        ))

        order = orderBook.getOrder(BigDecimal.valueOf(1.0), false, true)
        assertNotNull(order)
        assertEquals("order1", order!!.externalId)

        order = orderBook.getOrder(BigDecimal.valueOf(1.14), false, false)
        assertNotNull(order)
        assertEquals("order2", order!!.externalId)
    }
}