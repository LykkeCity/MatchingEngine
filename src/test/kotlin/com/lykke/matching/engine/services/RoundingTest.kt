package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import kotlin.test.assertNotNull

class RoundingTest: AbstractTest() {

    companion object {
        private const val DELTA = 1e-9
    }

    @Before
    fun setUp() {
        testOrderDatabaseAccessor.clear()
        testWalletDatabaseAccessor.clear()
        tradesInfoQueue.clear()

        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 0))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCLKK", "BTC", "LKK", 2))
    }

    @After
    fun tearDown() {
    }

    @Test
    fun testStraightBuy() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1.0)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(1.11548, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("1.12", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("1.00", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("EUR", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(999.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.12, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1498.88, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testStraightSell() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 1500.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1.0)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(1.11548, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("1.00", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("1.11", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(998.89, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1.11, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testNotStraightBuy() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 1500.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1.0, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(1.11548, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.90", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("1.00", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(999.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.9, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
        Assert.assertEquals(1499.1, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
    }

    @Test
    fun testNotStraightSell() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1.0, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(1.11548, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("1.00", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("0.89", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("EUR", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(999.11, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.89, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testNotStraightSellRoundingError() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 909.727, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "CHF", 1.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCCHF", volume = 	-0.38, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(909.727, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.38", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("CHF", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("0.00041770", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("BTC", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(0.9995823, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(0.38, testWalletDatabaseAccessor.getBalance("Client3", "CHF"), DELTA)
        Assert.assertEquals(0.0004177, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(0.62, testWalletDatabaseAccessor.getBalance("Client4", "CHF"), DELTA)
    }

    @Test
    fun testStraightBuyBTC() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = 1.0)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(678.229, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("678.23", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("1.00000000", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("BTC", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(999.0, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(678.23, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(821.77, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testStraightSellBTC() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1500.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = -1.0)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(678.229, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("1.00000000", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("678.22", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(321.78, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(678.22, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testNotStraightBuyBTC() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1500.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = 1.0, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(678.229, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.00147443", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("1.00", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(999.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.00147443, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
        Assert.assertEquals(1499.99852557, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
    }

    @Test
    fun testNotStraightSellBTC() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = -1.0, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(678.229, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("1.00", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("0.00147442", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("BTC", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(999.99852558, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.00147442, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testNotStraightSellBTCMultiLevel() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14925.09, volume = -1.34, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14950.18, volume = -1.34, clientId = "Client3"))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14975.27, volume = -1.34, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "LKK", 50800.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCLKK", volume = -50800.0, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(14945.93, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(3, marketOrderReport.trades.size)

        Assert.assertEquals(50800.0, testWalletDatabaseAccessor.getBalance("Client3", "LKK"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "LKK"), DELTA)
    }

    @Test
    fun testNotStraightBuyEURJPY() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 116.356, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "JPY", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 0.00999999999999999))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURJPY", volume = 1.16, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.NotEnoughFunds.name, marketOrderReport.order.status)
    }

    @Test
    fun testStraightSellBTCEUR() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 597.169, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCEUR", volume = -0.0001)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(597.169, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.00010000", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("0.05", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("EUR", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(0.0001, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(0.95, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(0.9999, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(0.05, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
    }

    @Test
    fun testLimitOrderRounding() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 1121.509, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1.0))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCEUR", volume = -0.00043722)))

        val limitOrder = testOrderDatabaseAccessor.getLastOrder("BTCEUR", true)
        assertNotNull(limitOrder)
        Assert.assertEquals(1000.0 - 0.00043722, limitOrder!!.remainingVolume, DELTA)
    }
}