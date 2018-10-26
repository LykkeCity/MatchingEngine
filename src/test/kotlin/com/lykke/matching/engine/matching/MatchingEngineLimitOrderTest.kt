package com.lykke.matching.engine.matching

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstructions
import com.lykke.matching.engine.utils.assertEquals
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MatchingEngineTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MatchingEngineLimitOrderTest : MatchingEngineTest() {

    @Test
    fun testMatchLimitOrderBuyWithEmptyOrderBook() {
        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchLimitOrderSellWithEmptyOrderBook() {
        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, remainingVolume = BigDecimal.valueOf(-100.0), status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchLimitOrderWithSameOrderBookSide() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, orderBookSize = 1, status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchNoLiquidityLimitOrderBuy() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, orderBookSize = 1, status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchNoLiquidityLimitOrderSell() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, remainingVolume = BigDecimal.valueOf(-100.0), orderBookSize = 1, status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchLimitOrderWithAnotherAssetPair() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 100.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("BTCUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, orderBookSize = 1, status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchLimitOrderWithOwnLimitOrder() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 100.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, skipSize = 1, status = OrderStatus.InOrderBook)
        assertEquals(1, getOrderBook("EURUSD", false).size)
    }

    @Test
    fun testMatchLimitOrderSellNotEnoughFundsOpposite() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 119.99)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, cancelledSize = 1, remainingVolume = BigDecimal.valueOf(-100.0), status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchLimitOrderBuyNotEnoughFundsOpposite() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 99.99)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, cancelledSize = 1, status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchLimitOrderBuyNotEnoughFundsOpposite2() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 99.99)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, cancelledSize = 1, status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchLimitOrderSellNotEnoughFundsOpposite2() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 120.00)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 119.99)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, cancelledSize = 1, remainingVolume = BigDecimal.valueOf(-100.0), status = OrderStatus.InOrderBook)
    }

    @Test
    fun testMatchLimitOrderBuyNotEnoughFunds() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 110.00)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null)
    }

    @Test
    fun testMatchLimitOrderSellNotEnoughFunds() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 99.00)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null, remainingVolume = BigDecimal.valueOf(-100.0))
    }

    @Test
    fun testMatchLimitOrderBuyNotEnoughFunds2() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 120.00)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 10.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null)
    }

    @Test
    fun testMatchLimitOrderSellNotEnoughFunds2() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 1.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null, remainingVolume = BigDecimal.valueOf(-100.0))
    }

    @Test
    fun testMatchLimitOrderBuyOneToOne1() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "uncompleted", clientId = "Client2", price = 1.19, volume = -100.0, reservedVolume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.21, volume = 91.1)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(891.59),
                remainingVolume = BigDecimal.ZERO,
                skipSize = 0,
                cancelledSize = 0,
                lkkTradesSize = 2,
                cashMovementsSize = 4,
                marketOrderTradesSize = 1,
                completedLimitOrdersSize = 0,
                limitOrdersReportSize = 1)

        val uncompletedLimitOrder = matchingResult.uncompletedLimitOrder
        assertNotNull(uncompletedLimitOrder)
        assertEquals("uncompleted", uncompletedLimitOrder!!.id)
        assertEquals(OrderStatus.Processing.name, uncompletedLimitOrder.status)
        assertEquals(BigDecimal.valueOf(-8.9), uncompletedLimitOrder.remainingVolume)
        assertEquals(BigDecimal.valueOf(8.9), uncompletedLimitOrder.reservedLimitVolume!!)
    }

    @Test
    fun testMatchLimitOrderSellOneToOne1() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "uncompleted", price = 1.21, volume = 108.1, reservedVolume = 130.81))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.19, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(900.00),
                remainingVolume = BigDecimal.ZERO,
                skipSize = 0,
                cancelledSize = 0,
                lkkTradesSize = 2,
                cashMovementsSize = 4,
                marketOrderTradesSize = 1,
                completedLimitOrdersSize = 0,
                limitOrdersReportSize = 1)

        val uncompletedLimitOrder = matchingResult.uncompletedLimitOrder
        assertNotNull(uncompletedLimitOrder)
        assertEquals("uncompleted", uncompletedLimitOrder!!.id)
        assertEquals(OrderStatus.Processing.name, uncompletedLimitOrder.status)
        assertEquals(BigDecimal.valueOf(8.1), uncompletedLimitOrder.remainingVolume)
        assertEquals(BigDecimal.valueOf(9.81), uncompletedLimitOrder.reservedLimitVolume!!)
    }

    @Test
    fun testMatchLimitOrderBuyOneToOne2() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 89.1)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "completed", clientId = "Client2", price = 1.19, volume = -89.1, reservedVolume = 89.1))
        initService()

        val limitOrder = buildLimitOrder(price = 1.21, volume = 91.1)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Processing,
                marketBalance = BigDecimal.valueOf(893.97),
                remainingVolume = BigDecimal.valueOf(2.0),
                skipSize = 0,
                cancelledSize = 0,
                lkkTradesSize = 2,
                cashMovementsSize = 4,
                marketOrderTradesSize = 1,
                completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client1", "EUR", now, BigDecimal.valueOf(89.1), BigDecimal.ZERO),
                        WalletOperation("", null, "Client1", "USD", now, BigDecimal.valueOf(-106.03), BigDecimal.ZERO)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client2", "EUR", now, BigDecimal.valueOf(-89.1), BigDecimal.valueOf(-89.1)),
                        WalletOperation("", null, "Client2", "USD", now, BigDecimal.valueOf(106.03), BigDecimal.ZERO)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testMatchLimitOrderSellOneToOne2() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 110.24)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "completed", price = 1.21, volume = 91.1, reservedVolume = 110.24))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.19, volume = -92.2)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Processing,
                marketBalance = BigDecimal.valueOf(908.9),
                remainingVolume = BigDecimal.valueOf(-1.1),
                skipSize = 0,
                cancelledSize = 0,
                lkkTradesSize = 2,
                cashMovementsSize = 5,
                marketOrderTradesSize = 1,
                completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client2", "EUR", now, BigDecimal.valueOf(-91.1), BigDecimal.ZERO),
                        WalletOperation("", null, "Client2", "USD", now, BigDecimal.valueOf(110.23), BigDecimal.ZERO)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client1", "EUR", now, BigDecimal.valueOf(91.1), BigDecimal.ZERO),
                        WalletOperation("", null, "Client1", "USD", now, BigDecimal.valueOf(-110.23), BigDecimal.valueOf(-110.23)),
                        WalletOperation("", null, "Client1", "USD", now, BigDecimal.ZERO, BigDecimal.valueOf(-0.01))
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testMatchLimitOrderBuyOneToOneFully() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "completed", clientId = "Client2", price = 1.2, volume = -100.0, reservedVolume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(880.0),
                remainingVolume = BigDecimal.ZERO, skipSize = 0, cancelledSize = 0, lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

    }

    @Test
    fun testMatchLimitOrderSellOneToOneFully() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "completed", price = 1.2, volume = 100.0, reservedVolume = 120.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(900.0),
                remainingVolume = BigDecimal.ZERO, skipSize = 0, cancelledSize = 0, lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

    }

    @Test
    fun testMatchLimitOrderBuyWithSeveral1() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -50.0, reservedVolume = 50.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -50.0, reservedVolume = 50.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(880.0), remainingVolume = BigDecimal.ZERO, skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 8, marketOrderTradesSize = 2, completedLimitOrdersSize = 2,
                limitOrdersReportSize = 2)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
    }

    @Test
    fun testMatchLimitOrderSellWithSeveral1() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 50.0, reservedVolume = 60.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 50.0, reservedVolume = 60.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(900.0), remainingVolume = BigDecimal.ZERO,
                skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 8, marketOrderTradesSize = 2, completedLimitOrdersSize = 2,
                limitOrdersReportSize = 2)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
    }

    @Test
    fun testMatchLimitOrderBuyWithSeveral2() {
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 40.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 40.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.1, volume = -40.0, reservedVolume = 40.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.15, volume = -40.0, reservedVolume = 40.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.2, volume = -40.0, reservedVolume = 40.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client4", price = 1.3, volume = -40.0, reservedVolume = 40.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult,
                status = OrderStatus.Processing,
                marketBalance = BigDecimal.valueOf(908.0),
                remainingVolume = BigDecimal.valueOf(20.0),
                skipSize = 1,
                cancelledSize = 0,
                lkkTradesSize = 4,
                cashMovementsSize = 8,
                marketOrderTradesSize = 2,
                completedLimitOrdersSize = 2,
                limitOrdersReportSize = 2,
                orderBookSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
        assertEquals("Client1", matchingResult.skipLimitOrders.first().clientId)
    }

    @Test
    fun testMatchLimitOrderSellWithSeveral2() {
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 60.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 60.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client4", price = 1.3, volume = 40.0, reservedVolume = 52.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.25, volume = 40.0, reservedVolume = 50.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.2, volume = 40.0, reservedVolume = 48.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.1, volume = 40.0, reservedVolume = 44.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test")

        assertLimitOrderMatchingResult(matchingResult,
                status = OrderStatus.Processing,
                marketBalance = BigDecimal.valueOf(920.0),
                remainingVolume = BigDecimal.valueOf(-20.0),
                skipSize = 1,
                cancelledSize = 0,
                lkkTradesSize = 4,
                cashMovementsSize = 8,
                marketOrderTradesSize = 2,
                completedLimitOrdersSize = 2,
                limitOrdersReportSize = 2,
                orderBookSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
        assertEquals("Client2", matchingResult.skipLimitOrders.first().clientId)
    }

    @Test
    fun testMatchWithSeveralLimitOrdersOfSameClient1() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 29.99)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 29.99)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 190000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "BTC", 0.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", volume = -29.98, price = 6100.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "limit-order-1", assetId = "BTCUSD", volume = -0.01, price = 6105.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.1, price = 6110.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 6110.0, volume = 30.0)
        Thread.sleep(100)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("BTCUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(6999.85),
                remainingVolume = BigDecimal.ZERO,
                skipSize = 0,
                cancelledSize = 0,
                lkkTradesSize = 6,
                cashMovementsSize = 12,
                marketOrderTradesSize = 3,
                completedLimitOrdersSize = 2,
                limitOrdersReportSize = 3)

        assertNotNull(matchingResult.uncompletedLimitOrder)
        assertNotNull(matchingResult.uncompletedLimitOrder!!.lastMatchTime)
        assertTrue { matchingResult.uncompletedLimitOrder!!.lastMatchTime!! > now }
        matchingResult.limitOrdersReport!!.orders.forEach {
            assertNotNull(it.order.lastMatchTime)
            assertTrue { it.order.lastMatchTime!! > now }
        }
        assertEquals(matchingResult.order.externalId, limitOrder.externalId)
        assertNotNull(limitOrder.lastMatchTime)
        assertTrue { limitOrder.lastMatchTime!! > now }
        assertEquals(BigDecimal.valueOf(-0.09), matchingResult.uncompletedLimitOrder!!.remainingVolume)
    }

    @Test
    fun testMatchWithSeveralLimitOrdersOfSameClient2() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 29.98)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 190000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "BTC", 0.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", volume = -29.98, price = 6100.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "limit-order-1", assetId = "BTCUSD", volume = -0.01, price = 6105.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.1, price = 6110.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 6110.0, volume = 30.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("BTCUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(6999.80),
                remainingVolume = BigDecimal.ZERO,
                skipSize = 0,
                cancelledSize = 1,
                lkkTradesSize = 4,
                cashMovementsSize = 8,
                marketOrderTradesSize = 2,
                completedLimitOrdersSize = 1,
                limitOrdersReportSize = 2)

        assertEquals(1, matchingResult.completedLimitOrders.size)
        assertNotNull(matchingResult.uncompletedLimitOrder)
        assertEquals(BigDecimal.valueOf(-0.08), matchingResult.uncompletedLimitOrder!!.remainingVolume)
    }

    @Test
    fun testTradesAfterMatching() {
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 52.33)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.25677, volume = -51.21, reservedVolume = 51.21))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.30001, volume = -52.33, reservedVolume = 52.33))
        initService()

        val limitOrder = buildLimitOrder(price = 1.31, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(872.21),
                remainingVolume = BigDecimal.ZERO, skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 8, marketOrderTradesSize = 2, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 2)

        val expectedLkkTrades = listOf(
                LkkTrade("EURUSD", "Client1", BigDecimal.valueOf(1.25677), BigDecimal.valueOf(51.21), now),
                LkkTrade("EURUSD", "Client2", BigDecimal.valueOf(1.25677), BigDecimal.valueOf(-51.21), now),
                LkkTrade("EURUSD", "Client1", BigDecimal.valueOf(1.30001), BigDecimal.valueOf(48.79), now),
                LkkTrade("EURUSD", "Client3", BigDecimal.valueOf(1.30001), BigDecimal.valueOf(-48.79), now)
        )

        assertNotNull(matchingResult.uncompletedLimitOrder)
        assertLkkTradesEquals(expectedLkkTrades, matchingResult.lkkTrades)
    }

    @Test
    fun testMatchLimitOrderSellFullBalance() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK1Y", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("LKK1YLKK", "LKK1Y", "LKK", 4))

        testBalanceHolderWrapper.updateBalance("Client1", "LKK1Y", 5495.03)
        testBalanceHolderWrapper.updateBalance("Client2", "LKK", 10000.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "LKK1YLKK", volume = 4.97, price = 1.0105))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "LKK1YLKK", volume = 5500.0, price = 1.0085))

        initService()

        val matchingResult = matchingEngine.match(buildLimitOrder(clientId = "Client1", assetId = "LKK1YLKK", volume = -5495.03, price = 1.0082,
                fees = buildLimitOrderFeeInstructions(type = FeeType.CLIENT_FEE, takerSize = 0.0009, targetClientId = "Client5")), getOrderBook("LKK1YLKK", true), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.ZERO,
                remainingVolume = BigDecimal.ZERO, skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 10, marketOrderTradesSize = 2, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 2)
    }

    @Test
    fun testMatchLimitOrderWithZeroLatestTrade() {
        testBalanceHolderWrapper.updateBalance("Client1", "CHF", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "CHF", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.001)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCCHF", price = 0.231, volume = 1.0, reservedVolume = 0.24))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", assetId = "BTCCHF", price = 0.231, volume = -0.001)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("BTCCHF", true), "test")

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.InOrderBook,
                marketBalance = BigDecimal.valueOf(0.001),
                remainingVolume = BigDecimal.valueOf(-0.001),
                skipSize = 1,
                matchedWithZeroLatestTrade = true)
    }

    @Test
    fun testMatchingSellOrderPriceDeviation() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.7, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = -1.0))

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.3, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1.0))
        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test", BigDecimal("1.53"), BigDecimal("1.57"))
        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.TooHighPriceDeviation, marketBalance = null, remainingVolume = BigDecimal.valueOf(-100.0))
    }

    @Test
    fun testMatchingBuyOrderPriceDeviation() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.7, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = -1.0))

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.3, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1.0))

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.7, volume = 5.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test", BigDecimal("1.53"), BigDecimal("1.57"), BigDecimal.valueOf(100))
        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.TooHighPriceDeviation, marketBalance = null, remainingVolume = BigDecimal.valueOf(5.0))
    }

    @Test
    fun testMatchingBuyOrderPriceDeviationWithSkipOrders() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.7, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.65, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = -1.0))

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.3, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1.0))

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.7, volume = 5.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false), "test", BigDecimal("1.53"), BigDecimal("1.57"), BigDecimal.valueOf(100))
        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.TooHighPriceDeviation, marketBalance = null, remainingVolume = BigDecimal.valueOf(5.0))
    }

    @Test
    fun testMatchingSellOrderPriceDeviationWithSkipOrders() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.7, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = -1.0))

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.3, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.3, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1.0))

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true), "test", BigDecimal("1.53"), BigDecimal("1.57"))
        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.TooHighPriceDeviation, marketBalance = null, remainingVolume = BigDecimal.valueOf(-100.0))
    }
}