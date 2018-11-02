package com.lykke.matching.engine.matching

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
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
class MatchingEngineMarketOrderTest : MatchingEngineTest() {

    @Test
    fun testMatchMarketOrderBuyWithEmptyOrderBook() {
        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderSellWithEmptyOrderBook() {
        val marketOrder = buildMarketOrder(volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderNotStraightBuyWithEmptyOrderBook() {
        val marketOrder = buildMarketOrder(volume = -100.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderNotStraightSellWithEmptyOrderBook() {
        val marketOrder = buildMarketOrder(volume = 100.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderWithSameOrderBookSide() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderNotStraightWithSameOrderBookSide() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderBuy() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.01)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderNotStraightBuy() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.3, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -130.01, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderSell() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.01)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderNotStraightSell() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 120.01, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchNoLiquidityMarketOrderNotStraightSell2() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.000028)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 1000.0, volume = 0.00001))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 1000.0, volume = 0.000009))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 1000.0, volume = 0.000009))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.02, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderWithAnotherAssetPair() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 100.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("BTCUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult)
    }

    @Test
    fun testMatchMarketOrderWithOwnLimitOrder() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 100.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult)
        assertEquals(1, getOrderBook("EURUSD", false).size)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFundsOpposite() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 99.99)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderNotStraightBuyNotEnoughFundsOpposite() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 99.99)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFundsOpposite() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 119.99)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFundsOpposite2() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 99.99)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFundsOpposite2() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 120.00)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 119.99)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, cancelledSize = 1)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFunds() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 110.00)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFunds() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 99.00)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderNotStraightSellNotEnoughFunds() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 99.00)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFunds2() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 120.00)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 10.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFunds2() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 1.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFunds3() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 120.00)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0, reservedVolume = 120.01)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderSellNotEnoughFunds3() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 100.00)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0, reservedVolume = 100.01)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.ReservedVolumeGreaterThanBalance, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderPriceDeviation() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.1, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.0, volume = 1.0))

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -3.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true),"test", priceDeviationThreshold = BigDecimal.valueOf(0.08))
        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.TooHighPriceDeviation, marketBalance = null)
    }

    @Test
    fun testMatchNotStraightMarketOrderPriceDeviation() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.1, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.0, volume = 1.0))

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 3.3, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true),"test", priceDeviationThreshold = BigDecimal.valueOf(0.08))
        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.TooHighPriceDeviation, marketBalance = null)
    }

    @Test
    fun testMatchMarketOrderBuyOneToOne() {
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 100.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "uncompleted", clientId = "Client2", price = 1.19, volume = -100.0, reservedVolume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 91.1)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(891.59),
                marketPrice = BigDecimal.valueOf(1.19),
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

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("Client1", "EUR", BigDecimal.valueOf(91.1), BigDecimal.ZERO),
                        WalletOperation("Client1", "USD", BigDecimal.valueOf(-108.41), BigDecimal.ZERO)
                ),
                matchingResult.ownCashMovements
        )

        assertCashMovementsEquals(
                listOf(
                        WalletOperation("Client2", "EUR", BigDecimal.valueOf(-91.1), BigDecimal.valueOf(-91.1)),
                        WalletOperation("Client2", "USD", BigDecimal.valueOf(108.41), BigDecimal.ZERO)
                ),
                matchingResult.oppositeCashMovements
        )
    }

    @Test
    fun testMatchMarketOrderSellOneToOne() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "uncompleted", price = 1.21, volume = 108.1, reservedVolume = 130.81))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(900.00),
                marketPrice = BigDecimal.valueOf(1.21),
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
    fun testMatchMarketOrderNotStraightBuyOneToOne() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "uncompleted", clientId = "Client2", price = 1.19, volume = -100.0, reservedVolume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -100.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(900.00),
                marketPrice = BigDecimal.valueOf(1.19),
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
        assertEquals(BigDecimal.valueOf(-15.9664), uncompletedLimitOrder.remainingVolume)
        assertEquals(BigDecimal.valueOf(15.9664), uncompletedLimitOrder.reservedLimitVolume!!)
    }

    @Test
    fun testMatchMarketOrderNotStraightSellOneToOne() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "uncompleted", price = 1.21, volume = 108.1, reservedVolume = 130.81))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 100.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(917.3553),
                marketPrice = BigDecimal.valueOf(1.21),
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
        assertEquals(BigDecimal.valueOf(25.4553), uncompletedLimitOrder.remainingVolume)
        assertEquals(BigDecimal.valueOf(30.81), uncompletedLimitOrder.reservedLimitVolume!!)
    }

    @Test
    fun testMatchMarketOrderBuyOneToOneFully() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "completed", clientId = "Client2", price = 1.2, volume = -100.0, reservedVolume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(880.0), marketPrice = BigDecimal.valueOf(1.2),
                lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

    }

    @Test
    fun testMatchMarketOrderNotStraightBuyOneToOneFully() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "completed", clientId = "Client2", price = 1.2, volume = -100.0, reservedVolume = 100.0))
        initService()

        val marketOrder = buildMarketOrder(volume = -120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(880.0),
                marketPrice = BigDecimal.valueOf(1.2), lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)

    }

    @Test
    fun testMatchMarketOrderSellOneToOneFully() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "completed", price = 1.2, volume = 100.0, reservedVolume = 120.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = -100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(900.0),
                marketPrice = BigDecimal.valueOf(1.2), lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)
    }

    @Test
    fun testMatchMarketOrderNotStraightSellOneToOneFully() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "completed", price = 1.2, volume = 100.0, reservedVolume = 120.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 120.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(900.0),
                marketPrice = BigDecimal.valueOf(1.2), lkkTradesSize = 2, cashMovementsSize = 4, marketOrderTradesSize = 1, completedLimitOrdersSize = 1,
                limitOrdersReportSize = 1)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders)
    }

    @Test
    fun testMatchMarketOrderBuyWithSeveral1() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -50.0, reservedVolume = 50.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.2, volume = -50.0, reservedVolume = 50.0))
        initService()

        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.Matched, marketBalance = BigDecimal.valueOf(880.0),
                marketPrice = BigDecimal.valueOf(1.2), skipSize = 0, cancelledSize = 0, lkkTradesSize = 4,
                cashMovementsSize = 8, marketOrderTradesSize = 2, completedLimitOrdersSize = 2,
                limitOrdersReportSize = 2)

        assertCompletedLimitOrders(matchingResult.completedLimitOrders, false)
    }

    @Test
    fun testMatchMarketOrderBuyWithSeveral2() {
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 40.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 40.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.1, volume = -40.0, reservedVolume = 40.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.15, volume = -40.0, reservedVolume = 40.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.2, volume = -40.0, reservedVolume = 40.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client4", price = 1.3, volume = -40.0, reservedVolume = 40.0))
        initService()

        Thread.sleep(100)
        val marketOrder = buildMarketOrder(volume = 100.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test")

        assertMarketOrderMatchingResult(matchingResult,
                status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(882.0),
                marketPrice = BigDecimal.valueOf(1.18),
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
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 60.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 60.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client4", price = 1.3, volume = 40.0, reservedVolume = 52.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", price = 1.25, volume = 40.0, reservedVolume = 50.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", price = 1.21234, volume = 40.0, reservedVolume = 48.5))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", price = 1.1, volume = 40.0, reservedVolume = 44.0))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client2", volume = 122.0, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test")

        assertMarketOrderMatchingResult(matchingResult,
                status = OrderStatus.Matched,
                marketBalance = BigDecimal.valueOf(900.4454),
                marketPrice = BigDecimal.valueOf(1.22546),
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
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.00952774, price = 10495.66))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.00947867, price = 10550.0, reservedVolume = 0.00947867))
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client3", assetId = "BTCUSD", volume = -100.0, straight = false)
        matchingEngine.match(marketOrder, getOrderBook("BTCUSD", false), "test")

        val orders = testDatabaseAccessor.getOrders("BTCUSD", false).filter { it.clientId == "Client1" }
        assertEquals(1, orders.size)
        assertEquals(BigDecimal.valueOf(0.00947867), orders.first().reservedLimitVolume)
    }

    @Test
    fun testNotStraightSellRoundingError() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 909.727, volume = -1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client4", "CHF", 1.0)
        initService()

        val marketOrder = buildMarketOrder(clientId = "Client4", assetId = "BTCCHF", volume = -0.3772, straight = false)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("BTCCHF", false), "test")

        assertEquals(1, matchingResult.marketOrderTrades.size)
        assertEquals("0.00041770", matchingResult.marketOrderTrades.first().limitVolume)
        assertEquals("0.38", matchingResult.marketOrderTrades.first().marketVolume)
    }

    @Test
    fun testMatchMarketOrderBuyNotEnoughFunds4() {
        testBalanceHolderWrapper.updateBalance("Client1", "CHF", 100.00)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.02)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 5999.0, volume = -0.005, clientId = "Client2"))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 6007.991, volume = -0.01165173, clientId = "Client2"))

        initService()

        val marketOrder = buildMarketOrder(clientId = "Client1", assetId = "BTCCHF", volume = 0.01665173)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("BTCCHF", false), "test")

        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.NotEnoughFunds)
    }

    @Test
    fun testMatchingSellOrderMidPriceDeviation() {
        //given
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 100.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 100.0)

        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 100.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 100.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.7, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = -1.0))

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.3, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.3, volume = 0.5))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 0.5))

        //when
        val marketOrder = buildMarketOrder(clientId = "Client2", assetId = "EURUSD", volume = -1.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", true), "test", BigDecimal("1.5"), BigDecimal("1.6"))

        //then
        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.TooHighMidPriceDeviation)
    }

    @Test
    fun testMatchingBuyOrderMidPriceDeviation() {
        //given
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 100.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 100.0)

        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 100.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 100.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.7, volume = -0.5))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.6, volume = -0.5))

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.3, volume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1.0))

        //when
        val marketOrder = buildMarketOrder(clientId = "Client2", assetId = "EURUSD", volume = 1.0)
        val matchingResult = matchingEngine.match(marketOrder, getOrderBook("EURUSD", false), "test", BigDecimal("1.5"), BigDecimal("1.57"))

        //then
        assertMarketOrderMatchingResult(matchingResult, status = OrderStatus.TooHighMidPriceDeviation)
    }
}