package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import org.junit.Assert.assertEquals
import org.junit.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class MatchingEngineMarketOrderTest : MatchingEngineTest() {

    @Test
    fun testMatchMarketOrderBuyWithEmptyOrderBook() {
        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderSellWithEmptyOrderBook() {
        val marketOrder = buildMarketOrder(volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderNotStraightBuyWithEmptyOrderBook() {
        val marketOrder = buildMarketOrder(volume = -100.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderNotStraightSellWithEmptyOrderBook() {
        val marketOrder = buildMarketOrder(volume = 100.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderWithSameOrderBookSide() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderNotStraightWithSameOrderBookSide() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderBuy() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.01)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderNotStraightBuy() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -130.01, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderSell() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.01)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderNotStraightSell() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 120.01, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderNotStraightSell2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.000028))

        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 1000.0, volume = 0.00001))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 1000.0, volume = 0.000009))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 1000.0, volume = 0.000009))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.02, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderWithAnotherAssetPair() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("BTCUSD", false))

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderWithOwnLimitOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult)
        assertEquals(1, getOrderBook("EURUSD", false).size)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFundsOpposite() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 99.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderNotStraightBuyNotEnoughFundsOpposite() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 99.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFundsOpposite() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 119.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFundsOpposite2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 100.00, reservedBalance = 99.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFundsOpposite2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 120.00, reservedBalance = 119.99))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFunds() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 110.00))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFunds() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 99.00))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderNotStraightSellNotEnoughFunds() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 99.00))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFunds2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 120.00, reservedBalance = 10.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFunds2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 100.00, reservedBalance = 1.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFunds3() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 120.00))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0, reservedVolume = 120.01)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFunds3() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 100.00))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0, reservedVolume = 100.01)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderBuyOneToOne() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 1000.0, 100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "uncompleted", clientId = "Client2", price = 1.19, volume = -100.0, reservedVolume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 91.1)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = 891.59,
                marketPrice = 1.19,
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

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client1", "EUR", now, 91.1, 0.0),
                        WalletOperation("", null, "Client1", "USD", now, -108.41, 0.0)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("", null, "Client2", "EUR", now, -91.1, -91.1),
                        WalletOperation("", null, "Client2", "USD", now, 108.41, 0.0)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testMatchMarketOrderSellOneToOne() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "uncompleted", price = 1.21, volume = 108.1, reservedVolume = 130.81))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = 900.00,
                marketPrice = 1.21,
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
    fun testMatchMarketOrderNotStraightBuyOneToOne() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "uncompleted", clientId = "Client2", price = 1.19, volume = -100.0, reservedVolume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -100.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = 900.00,
                marketPrice = 1.19,
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
        assertEquals(-15.9664, uncompletedLimitOrder.remainingVolume, DELTA)
        assertEquals(15.9664, uncompletedLimitOrder.reservedLimitVolume!!, DELTA)
    }

    @Test
    fun testMatchMarketOrderNotStraightSellOneToOne() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "uncompleted", price = 1.21, volume = 108.1, reservedVolume = 130.81))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 100.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = 917.3553,
                marketPrice = 1.21,
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
        assertEquals(25.4553, uncompletedLimitOrder.remainingVolume, DELTA)
        assertEquals(30.81, uncompletedLimitOrder.reservedLimitVolume!!, DELTA)
    }

    @Test
    fun testMatchMarketOrderBuyOneToOneFully() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "completed", clientId = "Client2", price = 1.2, volume = -100.0, reservedVolume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 880.0, marketPrice = 1.2,lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

    }

    @Test
    fun testMatchMarketOrderNotStraightBuyOneToOneFully() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "completed", clientId = "Client2", price = 1.2, volume = -100.0, reservedVolume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 880.0, marketPrice = 1.2, lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

    }

    @Test
    fun testMatchMarketOrderSellOneToOneFully() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "completed", price = 1.2, volume = 100.0, reservedVolume = 120.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 900.0, marketPrice = 1.2, lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)
    }

    @Test
    fun testMatchMarketOrderNotStraightSellOneToOneFully() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "completed", price = 1.2, volume = 100.0, reservedVolume = 120.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 900.0, marketPrice = 1.2, lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)
    }

    @Test
    fun testMatchMarketOrderBuyWithSeveral1() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -50.0, reservedVolume = 50.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -50.0, reservedVolume = 50.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = 880.0, marketPrice = 1.2, skipSize = 0, cancelledSize = 0, lkkTradesSize = 4, cashMovementsSize = 8, marketOrderTradesSize = 2, completedLimitOrdersSize = 2,
                limitOrdersReportSize = 2)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
    }

    @Test
    fun testMatchMarketOrderBuyWithSeveral2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 40.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 40.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.1, volume = -40.0, reservedVolume = 40.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.15, volume = -40.0, reservedVolume = 40.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.2, volume = -40.0, reservedVolume = 40.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", price = 1.3, volume = -40.0, reservedVolume = 40.0))
        initService()

        Thread.sleep(100)
        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false))

        assertMarketOrderMatchingResult(matchingResult,
                status = OrderStatus.Matched,
                marketBalance = 882.0,
                marketPrice = 1.18,
                skipSize = 1,
                cancelledSize = 0,
                lkkTradesSize = 6,
                cashMovementsSize = 12,
                marketOrderTradesSize = 3,
                completedLimitOrdersSize = 2,
                limitOrdersReportSize = 3,
                orderBookSize = 0)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
        assertEquals("Client1", matchingResult.skipLimitOrders.first().clientId)

        assertEquals(matchingResult.order.externalId, marketOrder.externalId)
        assertNotNull(marketOrder.matchedAt)
        assertTrue { marketOrder.matchedAt!! > now }
        assertNotNull(matchingResult.uncompletedLimitOrder)
    }

    @Test
    fun testMatchMarketOrderSellWithSeveral() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 60.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 60.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client4", price = 1.3, volume = 40.0, reservedVolume = 52.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.25, volume = 40.0, reservedVolume = 50.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.21234, volume = 40.0, reservedVolume = 48.5))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.1, volume = 40.0, reservedVolume = 44.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 122.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true))

        assertMarketOrderMatchingResult(matchingResult,
                status = OrderStatus.Matched,
                marketBalance = 900.4454,
                marketPrice = 1.22546,
                skipSize = 1,
                cancelledSize = 0,
                lkkTradesSize = 6,
                cashMovementsSize = 13,
                marketOrderTradesSize = 3,
                completedLimitOrdersSize = 2,
                limitOrdersReportSize = 3,
                orderBookSize = 0)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
        assertEquals("Client2", matchingResult.skipLimitOrders.first().clientId)

        assertNotNull(matchingResult.uncompletedLimitOrder)
    }

    @Test
    fun testReservedVolume() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.00952774, price = 10495.66))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.00947867, price = 10550.0, reservedVolume = 0.00947867))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client3", assetId = "BTCUSD", volume = -100.0, straight = false)
        matchingEngine.match(marketOrder, getOrderBook("BTCUSD", false))

        val orders = testDatabaseAccessor.getOrders("BTCUSD", false).filter { it.clientId == "Client1" }
        assertEquals(1, orders.size)
        assertEquals(0.00947867, orders.first().reservedLimitVolume)
    }

    @Test
    fun testNotStraightSellRoundingError() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 909.727, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "CHF", 1.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client4", assetId = "BTCCHF", volume = -0.3772, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("BTCCHF", false))

        assertEquals(1, matchingResult.marketOrderTrades.size)
        assertEquals("0.00041770", matchingResult.marketOrderTrades.first().limitVolume)
        assertEquals("0.38", matchingResult.marketOrderTrades.first().marketVolume)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFunds4() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "CHF", 100.00))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.02))

        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 5999.0, volume = -0.005, clientId = "Client2"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 6007.991, volume = -0.01165173, clientId = "Client2"))

        initService()

        val marketOrder = buildMarketOrder(clientId = "Client1", assetId = "BTCCHF", volume = 0.01665173)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("BTCCHF", false))

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds)
    }
}