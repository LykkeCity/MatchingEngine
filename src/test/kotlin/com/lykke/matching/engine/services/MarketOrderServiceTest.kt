package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.concurrent.LinkedBlockingQueue

class MarketOrderServiceTest {
    val testDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    val testLimitDatabaseAccessor = TestFileOrderDatabaseAccessor()
    val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testDictionariesDatabaseAccessor, 60000))
    lateinit var balancesHolder: BalancesHolder
    lateinit var limitOrderService: GenericLimitOrderService
    lateinit var service: MarketOrderService

    val DELTA = 1e-9

    @Before
    fun setUp() {
        testLimitDatabaseAccessor.clear()
        testWalletDatabaseAccessor.clear()
        tradesInfoQueue.clear()

        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 0))
        testBackOfficeDatabaseAccessor.addAsset(Asset("SLR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("GBP", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC1", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCLKK", "BTC", "LKK", 6))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTC1USD", "BTC1", "USD", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("SLRBTC", "SLR", "BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("LKKEUR", "LKK", "EUR", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("LKKGBP", "LKK", "GBP", 5))
    }

    @After
    fun tearDown() {
    }

    fun initServices() {
        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, emptySet())
        limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
        service = MarketOrderService(testBackOfficeDatabaseAccessor, testDatabaseAccessor, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue)
    }

    @Test
    fun testNoLiqudity() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder()))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)
    }

    @Test
    fun testNotEnoughFundsClientOrder() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -1000.0)))
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
    }

    @Test
    fun testNotEnoughFundsClientMultiOrder() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -1500.0)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)
    }

    @Test
    fun testNoLiqudityToFullyFill() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 2000.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -2000.0)))
        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NoLiquidity.name, marketOrderReport.order.status)
    }

    @Test
    fun testNotEnoughFundsMarketOrder() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 900.0))
        initServices()
        
        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))
        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NotEnoughFunds.name, marketOrderReport.order.status)
    }

    @Test
    fun testMatchOneToOne() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0, reservedBalance = 1500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 1000.0))
        initServices()

        assertEquals(1500.0, testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"), DELTA)
        
        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(1.5, marketOrderReport.order.price!!, DELTA)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("1000.00", marketOrderReport.trades.first().marketVolume)
        assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("1500.00", marketOrderReport.trades.first().limitVolume)
        assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(1000.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(1500.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)

        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"), DELTA)
    }

    @Test
    fun testMatchOneToOneEURJPY() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 122.512, volume = 1000000.0, clientId = "Client3"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 122.524, volume = -1000000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "JPY", 5000000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 5000000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 0.1))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "JPY", 100.0))
        initServices()
        
        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURJPY", volume = 10.0, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(122.512, marketOrderReport.order.price!!, DELTA)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals("0.09", marketOrderReport.trades.first().marketVolume)
        assertEquals("EUR", marketOrderReport.trades.first().marketAsset)
        assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        assertEquals("10.00", marketOrderReport.trades.first().limitVolume)
        assertEquals("JPY", marketOrderReport.trades.first().limitAsset)
        assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        assertEquals(5000000.09, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(4999990.0, testWalletDatabaseAccessor.getBalance("Client3", "JPY"), DELTA)
        assertEquals(0.01, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(110.0, testWalletDatabaseAccessor.getBalance("Client4", "JPY"), DELTA)
    }

    @Test
    fun testMatchOneToOneAfterNotEnoughFunds() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        initServices()
        
        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        var marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(NotEnoughFunds.name, marketOrderReport.order.status)
        assertEquals(0, marketOrderReport.trades.size)

        assertEquals(NotEnoughFunds.name, marketOrderReport.order.status)

        balancesHolder.updateBalance("Client4", "EUR", Date(), 1000.0)
        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(1.5, marketOrderReport.order.price!!, DELTA)
        assertEquals(1, marketOrderReport.trades.size)
        assertEquals(Matched.name, marketOrderReport.order.status)

        assertEquals(1000.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(1500.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testMatchOneToMany() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 100.0, clientId = "Client3"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.4, volume = 1000.0, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1560.0, reservedBalance = 1400.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 150.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 1000.0))
        initServices()
        
        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(1.41, marketOrderReport.order.price!!, DELTA)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(100.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(900.0, testWalletDatabaseAccessor.getBalance("Client1", "EUR"), DELTA)
        assertEquals(300.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"), DELTA)
        assertEquals(140.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(1410.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)

        val dbBids = testLimitDatabaseAccessor.getOrders("EURUSD", true)
        assertEquals(1, dbBids.size)
        assertEquals(OrderStatus.Processing.name, dbBids.first().status)
    }

    @Test
    fun testMatchOneToMany2016Nov10() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04412, volume = -20000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04421, volume = -20000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKEUR", price = 0.04431, volume = -20000.0, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "LKK", 6569074.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 7500.02))
        initServices()
        
        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "LKKEUR", volume = 50000.0)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(0.0442, marketOrderReport.order.price!!, DELTA)
        assertEquals(3, marketOrderReport.trades.size)

        assertEquals(2209.7, testWalletDatabaseAccessor.getBalance("Client1", "EUR"), DELTA)
        assertEquals(6519074.0, testWalletDatabaseAccessor.getBalance("Client1", "LKK"), DELTA)
        assertEquals(5290.32, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(50000.0, testWalletDatabaseAccessor.getBalance("Client4", "LKK"), DELTA)
    }

    @Test
    fun testMatchOneToMany2016Nov10_2() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13611.625476, volume = 1.463935, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13586.531910, volume = 1.463935, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 13561.438344, volume = 1.463935, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "LKK", 100000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 12.67565686))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCLKK", volume = 50000.0, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(13591.395424, marketOrderReport.order.price!!, DELTA)
        assertEquals(3, marketOrderReport.trades.size)

        assertEquals(3.67889654, testWalletDatabaseAccessor.getBalance("Client1", "BTC"), DELTA)
        assertEquals(50000.0, testWalletDatabaseAccessor.getBalance("Client1", "LKK"), DELTA)
        assertEquals(8.99676032, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        assertEquals(50000.0, testWalletDatabaseAccessor.getBalance("Client4", "LKK"), DELTA)
    }

    @Test
    fun testMatchOneToMany2016Nov10_3() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKGBP", price = 0.0385, volume = -20000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "LKKGBP", price = 0.03859, volume = -20000.0, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "LKK", 100000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "GBP", 982.78))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "LKKGBP", volume = -982.78, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(0.03854, marketOrderReport.order.price!!, DELTA)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(982.78, testWalletDatabaseAccessor.getBalance("Client1", "GBP"), DELTA)
        assertEquals(74487.0, testWalletDatabaseAccessor.getBalance("Client1", "LKK"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "GBP"), DELTA)
        assertEquals(25513.0, testWalletDatabaseAccessor.getBalance("Client4", "LKK"), DELTA)
    }

    @Test
    fun testMatchOneToMany2016Dec12() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008826, volume = -4000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008844, volume = -4000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008861, volume = -4000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008879, volume = -4000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008897, volume = -4000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008914, volume = -4000.0, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "SLRBTC", price = 0.00008932, volume = -4000.0, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "SLR", 100000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 31.95294))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "SLRBTC", volume = 25000.0, straight = true)))

        assertEquals(2.21816, testWalletDatabaseAccessor.getBalance("Client1", "BTC"), DELTA)
        assertEquals(75000.0, testWalletDatabaseAccessor.getBalance("Client1", "SLR"), DELTA)
        assertEquals(29.73478, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        assertEquals(25000.0, testWalletDatabaseAccessor.getBalance("Client4", "SLR"), DELTA)
    }

    @Test
    fun testMatchOneToMany2016Dec12_2() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 791.37, volume = 4000.0, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "CHF", 100000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 0.00036983))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCCHF", volume = -0.00036983, straight = true)))

        assertEquals(0.00036983, testWalletDatabaseAccessor.getBalance("Client1", "BTC"), DELTA)
        assertEquals(99999.71, testWalletDatabaseAccessor.getBalance("Client1", "CHF"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        assertEquals(0.29, testWalletDatabaseAccessor.getBalance("Client4", "CHF"), DELTA)
    }

    @Test
    fun testNotStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = -500.0, assetId = "EURUSD", clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 750.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -750.0, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(1.5, marketOrderReport.order.price!!, DELTA)
        assertEquals(1, marketOrderReport.trades.size)

        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(750.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(500.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testNotStraightMatchOneToMany() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.4, volume = -100.0, clientId = "Client3"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = -1000.0, clientId = "Client1"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 3000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 3000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 2000.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1490.0, straight = false)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(1.49, marketOrderReport.order.price!!, DELTA)
        assertEquals(2, marketOrderReport.trades.size)

        assertEquals(2900.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(140.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(2100.0, testWalletDatabaseAccessor.getBalance("Client1", "EUR"), DELTA)
        assertEquals(1350.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"), DELTA)
        assertEquals(1000.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(510.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testMatch1() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 100028.39125545))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "CHF", 182207.39))

        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4071.121, volume = -0.00662454, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4077.641, volume = -0.01166889, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4084.382, volume = -0.01980138, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4091.837, volume = -0.02316231, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4098.155, volume = -0.03013115, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4105.411, volume = -0.03790487, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4114.279, volume = -0.03841106, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4120.003, volume = -0.04839733, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4127.137, volume = -0.04879837, clientId = "Client1"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 4136.9, volume = -0.06450525, clientId = "Client1"))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "BTCCHF", volume = 0.3)))

        assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(Matched.name, marketOrderReport.order.status)
        assertEquals(4111.117, marketOrderReport.order.price!!, DELTA)
        assertEquals(10, marketOrderReport.trades.size)

        assertEquals(4136.9, limitOrderService.getOrderBook("BTCCHF").getAskPrice(), DELTA)
    }
}
