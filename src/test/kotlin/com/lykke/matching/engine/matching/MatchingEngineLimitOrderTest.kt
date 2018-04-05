package com.lykke.matching.engine.matching

import com.lykke.matching.engine.config.TestApplicationContext
import org.junit.runner.RunWith

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstructions
import org.junit.Assert.assertEquals
import org.junit.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MatchingEngineTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MatchingEngineLimitOrderTest : MatchingEngineTest() {

    @Test
    fun testMatchLimitOrderBuyWithEmptyOrderBook() {
        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchLimitOrderSellWithEmptyOrderBook() {
        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, remainingVolume = -100.0)
    }

    @Test
    fun testMatchLimitOrderWithSameOrderBookSide() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, orderBookSize = 1)
    }

    @Test
    fun testMatchNoLiquidityLimitOrderBuy() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, orderBookSize = 1)
    }

    @Test
    fun testMatchNoLiquidityLimitOrderSell() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, remainingVolume = -100.0, orderBookSize = 1)
    }

    @Test
    fun testMatchLimitOrderWithAnotherAssetPair() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("BTCUSD", false))

        assertLimitOrderMatchingResult(matchingResult, orderBookSize = 1)
    }

    @Test
    fun testMatchLimitOrderWithOwnLimitOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, skipSize = 1)
        assertEquals(1, getOrderBook("EURUSD", false).size)
    }

    @Test
    fun testMatchLimitOrderSellNotEnoughFundsOpposite() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 119.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, cancelledSize = 1, remainingVolume = -100.0)
    }

    @Test
    fun testMatchLimitOrderBuyNotEnoughFundsOpposite() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 99.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchLimitOrderBuyNotEnoughFundsOpposite2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 100.00, reservedBalance = 99.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchLimitOrderSellNotEnoughFundsOpposite2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 120.00, reservedBalance = 119.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, cancelledSize = 1, remainingVolume = -100.0)
    }

    @Test
    fun testMatchLimitOrderBuyNotEnoughFunds() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 110.00))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null)
    }

    @Test
    fun testMatchLimitOrderSellNotEnoughFunds() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 99.00))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null, remainingVolume = -100.0)
    }

    @Test
    fun testMatchLimitOrderBuyNotEnoughFunds2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 120.00, reservedBalance = 10.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null)
    }

    @Test
    fun testMatchLimitOrderSellNotEnoughFunds2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 100.00, reservedBalance = 1.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null, remainingVolume = -100.0)
    }

    @Test
    fun testMatchLimitOrderBuyOneToOne1() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "uncompleted", clientId = "Client2", price = 1.19, volume = -100.0, reservedVolume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.21, volume = 91.1)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = 891.59,
                remainingVolume = 0.0,
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
        assertEquals(-8.9, uncompletedLimitOrder.remainingVolume, DELTA)
        assertEquals(8.9, uncompletedLimitOrder.reservedLimitVolume!!, DELTA)
    }

    @Test
    fun testMatchLimitOrderSellOneToOne1() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "uncompleted", price = 1.21, volume = 108.1, reservedVolume = 130.81))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.19, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = 900.00,
                remainingVolume = 0.0,
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
        assertEquals(8.1, uncompletedLimitOrder.remainingVolume, DELTA)
        assertEquals(9.81, uncompletedLimitOrder.reservedLimitVolume!!, DELTA)
    }

    @Test
    fun testMatchLimitOrderBuyOneToOne2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 1000.0, 89.1))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "completed", clientId = "Client2", price = 1.19, volume = -89.1, reservedVolume = 89.1))
        initService()

        val limitOrder = buildLimitOrder(price = 1.21, volume = 91.1)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Processing,
                marketBalance = 893.97,
                remainingVolume = 2.0,
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
                        WalletOperation("", null, "Client1", "EUR", now, 89.1, 0.0),
                        WalletOperation("", null, "Client1", "USD", now, -106.03, 0.0)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client2", "EUR", now, -89.1, -89.1),
                        WalletOperation("", null, "Client2", "USD", now, 106.03, 0.0)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testMatchLimitOrderSellOneToOne2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0, 110.24))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "completed", price = 1.21, volume = 91.1, reservedVolume = 110.24))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.19, volume = -92.2)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Processing,
                marketBalance = 908.9,
                remainingVolume = -1.1,
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
                        WalletOperation("", null, "Client2", "EUR", now, -91.1, 0.0),
                        WalletOperation("", null, "Client2", "USD", now, 110.23, 0.0)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client1", "EUR", now, 91.1, 0.0),
                        WalletOperation("", null, "Client1", "USD", now, -110.23, -110.23),
                        WalletOperation("", null, "Client1", "USD", now, 0.0, -0.01)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testMatchLimitOrderBuyOneToOneFully() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "completed", clientId = "Client2", price = 1.2, volume = -100.0, reservedVolume = 100.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 880.0, remainingVolume = 0.0, skipSize = 0, cancelledSize = 0, lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

    }

    @Test
    fun testMatchLimitOrderSellOneToOneFully() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "completed", price = 1.2, volume = 100.0, reservedVolume = 120.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 900.0, remainingVolume = 0.0, skipSize = 0, cancelledSize = 0, lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

    }

    @Test
    fun testMatchLimitOrderBuyWithSeveral1() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -50.0, reservedVolume = 50.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -50.0, reservedVolume = 50.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 880.0, remainingVolume = 0.0, skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 8, marketOrderTradesSize = 2, completedLimitOrdersSize = 2,
                limitOrdersReportSize = 2)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
    }

    @Test
    fun testMatchLimitOrderSellWithSeveral1() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 50.0, reservedVolume = 60.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 50.0, reservedVolume = 60.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 900.0, remainingVolume = 0.0, skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 8, marketOrderTradesSize = 2, completedLimitOrdersSize = 2,
                limitOrdersReportSize = 2)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
    }

    @Test
    fun testMatchLimitOrderBuyWithSeveral2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 40.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 40.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.1, volume = -40.0, reservedVolume = 40.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.15, volume = -40.0, reservedVolume = 40.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.2, volume = -40.0, reservedVolume = 40.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", price = 1.3, volume = -40.0, reservedVolume = 40.0))
        initService()

        val limitOrder = buildLimitOrder(price = 1.2, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult,
                status = OrderStatus.Processing,
                marketBalance = 908.0,
                remainingVolume = 20.0,
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
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 60.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 60.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", price = 1.3, volume = 40.0, reservedVolume = 52.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.25, volume = 40.0, reservedVolume = 50.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.2, volume = 40.0, reservedVolume = 48.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.1, volume = 40.0, reservedVolume = 44.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", true))

        assertLimitOrderMatchingResult(matchingResult,
                status = OrderStatus.Processing,
                marketBalance = 920.0,
                remainingVolume = -20.0,
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
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 100.00, reservedBalance = 29.99))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 190000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 100.00, reservedBalance = 0.0))

        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", volume = -29.98, price = 6100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "limit-order-1", assetId = "BTCUSD", volume = -0.01, price = 6105.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.1, price = 6110.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 6110.0, volume = 30.0)
        Thread.sleep(100)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("BTCUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = 6999.85,
                remainingVolume = 0.0,
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
        assertEquals(-0.09, matchingResult.uncompletedLimitOrder!!.remainingVolume, DELTA)
    }

    @Test
    fun testMatchWithSeveralLimitOrdersOfSameClient2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 100.00, reservedBalance = 29.98))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 190000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 100.00, reservedBalance = 0.0))

        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", volume = -29.98, price = 6100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "limit-order-1", assetId = "BTCUSD", volume = -0.01, price = 6105.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.1, price = 6110.0))
        initService()

        val limitOrder = buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 6110.0, volume = 30.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("BTCUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = 6999.80,
                remainingVolume = 0.0,
                skipSize = 0,
                cancelledSize = 1,
                lkkTradesSize = 4,
                cashMovementsSize = 8,
                marketOrderTradesSize = 2,
                completedLimitOrdersSize = 1,
                limitOrdersReportSize = 2)

        assertEquals(1, matchingResult.completedLimitOrders.size)
        assertNotNull(matchingResult.uncompletedLimitOrder)
        assertEquals(-0.08, matchingResult.uncompletedLimitOrder!!.remainingVolume, DELTA)
    }

    @Test
    fun testTradesAfterMatching() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 52.33))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.25677, volume = -51.21, reservedVolume = 51.21))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.30001, volume = -52.33, reservedVolume = 52.33))
        initService()

        val limitOrder = buildLimitOrder(price = 1.31, volume = 100.0)
        val matchingResult = matchingEngine.match(limitOrder, getOrderBook("EURUSD", false))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 872.21, remainingVolume = 0.0, skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 8, marketOrderTradesSize = 2, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 2)

        val expectedLkkTrades = listOf(
                LkkTrade("EURUSD", "Client1", 1.25677, 51.21, now),
                LkkTrade("EURUSD", "Client2", 1.25677, -51.21, now),
                LkkTrade("EURUSD", "Client1", 1.30001, 48.79, now),
                LkkTrade("EURUSD", "Client3", 1.30001, -48.79, now)
        )

        assertNotNull(matchingResult.uncompletedLimitOrder)
        assertLkkTradesEquals(expectedLkkTrades, matchingResult.lkkTrades)
    }

    @Test
    fun testMatchLimitOrderSellFullBalance() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK1Y", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("LKK1YLKK", "LKK1Y", "LKK", 4))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "LKK1Y", 5495.03))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "LKK", 10000.0))

        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "LKK1YLKK", volume = 4.97, price = 1.0105))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "LKK1YLKK", volume = 5500.0, price = 1.0085))

        initService()

        val matchingResult = matchingEngine.match(buildLimitOrder(clientId = "Client1", assetId = "LKK1YLKK", volume = -5495.03, price = 1.0082,
                fees = buildLimitOrderFeeInstructions(type = FeeType.CLIENT_FEE, takerSize = 0.0009, targetClientId = "Client5")), getOrderBook("LKK1YLKK", true))

        assertLimitOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 0.0, remainingVolume = 0.0, skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 10, marketOrderTradesSize = 2, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 2)
    }
}