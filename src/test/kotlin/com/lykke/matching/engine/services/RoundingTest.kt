package com.lykke.matching.engine.services

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletCredentials
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.queue.transaction.Swap
import com.lykke.matching.engine.queue.transaction.Transaction
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue

class RoundingTest {
    var testDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    var testLimitDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    var testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    val transactionQueue = LinkedBlockingQueue<Transaction>()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val walletCredentialsCache = WalletCredentialsCache(testBackOfficeDatabaseAccessor)

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAccessor, 60000))
    val balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue)

    var limitOrderService = GenericLimitOrderService(false, testLimitDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
    var service = MarketOrderService(testBackOfficeDatabaseAccessor, testDatabaseAccessor, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, transactionQueue, limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, rabbitSwapQueue, false)

    val DELTA = 1e-9

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testLimitDatabaseAccessor.clear()
        testWalletDatabaseAccessor.clear()
        transactionQueue.clear()
        tradesInfoQueue.clear()

        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2, "EUR"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2, "USD"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 2, "JPY"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8, "BTC"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2, "CHF"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 0, "LKK"))
        testBackOfficeDatabaseAccessor.addWalletCredentials(WalletCredentials("Client3", "Client3-Multisig"))
        testBackOfficeDatabaseAccessor.addWalletCredentials(WalletCredentials("Client4", "Client4-Multisig"))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 3, 6))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 3, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 3, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCLKK", "BTC", "LKK", 2, 8))

        this.walletCredentialsCache.reloadCache()
    }

    @After
    fun tearDown() {
    }

    fun initServices() {
        limitOrderService = GenericLimitOrderService(false, testLimitDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
        service = MarketOrderService(testBackOfficeDatabaseAccessor, testDatabaseAccessor, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, transactionQueue, limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, rabbitSwapQueue, false)
    }

    @Test
    fun testStraightBuy() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1.0)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(1.11548, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(1.12, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-1.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "EUR" }?.volume)
        Assert.assertEquals(-1.12, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(1.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "EUR" }?.volume)

        Assert.assertEquals(999.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.12, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1498.88, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(1.12, swap.Amount1, 0.0)
        Assert.assertEquals("USD", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(1.0, swap.Amount2, 0.0)
        Assert.assertEquals("EUR", swap.origAsset2)
    }

    @Test
    fun testStraightSell() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1.0)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(1.11548, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(-1.11, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(1.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "EUR" }?.volume)
        Assert.assertEquals(1.11, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-1.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "EUR" }?.volume)

        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(998.89, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1.11, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(1.0, swap.Amount1, 0.0)
        Assert.assertEquals("EUR", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(1.11, swap.Amount2, 0.0)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testNotStraightBuy() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(1.11548, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(-1.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(0.9, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "EUR" }?.volume)
        Assert.assertEquals(1.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-0.9, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "EUR" }?.volume)

        Assert.assertEquals(999.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.9, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
        Assert.assertEquals(1499.1, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.9, swap.Amount1, 0.0)
        Assert.assertEquals("EUR", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(1.0, swap.Amount2, 0.0)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testNotStraightSell() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(1.11548, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(1.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-0.89, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "EUR" }?.volume)
        Assert.assertEquals(-1.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(0.89, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "EUR" }?.volume)

        Assert.assertEquals(999.11, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.89, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(1.0, swap.Amount1, 0.0)
        Assert.assertEquals("USD", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(0.89, swap.Amount2, 0.0)
        Assert.assertEquals("EUR", swap.origAsset2)
    }

    @Test
    fun testNotStraightSellRoundingError() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCCHF", price = 909.727, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "CHF", 1.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCCHF", volume = 	-0.3772, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(909.727, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(0.38, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "CHF" }?.volume)
        Assert.assertEquals(-0.0004177, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(-0.38, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "CHF" }?.volume)
        Assert.assertEquals(0.0004177, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" }?.volume)

        Assert.assertEquals(0.9995823, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(0.38, testWalletDatabaseAccessor.getBalance("Client3", "CHF"), DELTA)
        Assert.assertEquals(0.0004177, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(0.62, testWalletDatabaseAccessor.getBalance("Client4", "CHF"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.38, swap.Amount1, 0.0)
        Assert.assertEquals("CHF", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(0.0004177, swap.Amount2, 0.0)
        Assert.assertEquals("BTC", swap.origAsset2)
    }

    @Test
    fun testStraightBuyBTC() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = 1.0)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(678.229, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(678.23, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-1.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(-678.23, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(1.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" }?.volume)

        Assert.assertEquals(999.0, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(678.23, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(821.77, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(678.23, swap.Amount1, 0.0)
        Assert.assertEquals("USD", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(1.0, swap.Amount2, 0.0)
        Assert.assertEquals("BTC", swap.origAsset2)
    }

    @Test
    fun testStraightSellBTC() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = -1.0)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(678.229, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(-678.22, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(1.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(678.22, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-1.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" }?.volume)

        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(321.78, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(678.22, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(1.0, swap.Amount1, 0.0)
        Assert.assertEquals("BTC", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(678.22, swap.Amount2, 0.0)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testNotStraightBuyBTC() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = 1.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(678.229, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(-1.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(0.00147443, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(1.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-0.00147443, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" }?.volume)

        Assert.assertEquals(999.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.00147443, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
        Assert.assertEquals(1499.99852557, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.00147443, swap.Amount1, 0.0)
        Assert.assertEquals("BTC", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(1.0, swap.Amount2, 0.0)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testNotStraightSellBTC() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = -1.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(678.229, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(1.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-0.00147442, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(-1.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(0.00147442, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" }?.volume)

        Assert.assertEquals(999.99852558, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.00147442, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(1.0, swap.Amount1, 0.0)
        Assert.assertEquals("USD", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(0.00147442, swap.Amount2, 0.0)
        Assert.assertEquals("BTC", swap.origAsset2)
    }

    @Test
    fun testNotStraightSellBTCMultiLevel() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14925.09, volume = -1.34, clientId = "Client3"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14950.18, volume = -1.34, clientId = "Client3"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCLKK", price = 14975.27, volume = -1.34, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "LKK", 50800.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCLKK", volume = -50800.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(14945.59, marketOrder.price!!, DELTA)

        Assert.assertEquals(50800.0, testWalletDatabaseAccessor.getBalance("Client3", "LKK"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "LKK"), DELTA)
    }

    @Test
    fun testNotStraightBuyEURJPY() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 116.356, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "JPY", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 0.00999999999999999))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURJPY", volume = 1.16, straight = false)))
        
        Assert.assertEquals(NotEnoughFunds.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testStraightSellBTCEUR() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 597.169, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCEUR", volume = -0.0001)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(597.169, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(-0.05, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "EUR" }?.volume)
        Assert.assertEquals(0.0001, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(0.05, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "EUR" }?.volume)
        Assert.assertEquals(-0.0001, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" }?.volume)

        Assert.assertEquals(0.0001, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(0.95, testWalletDatabaseAccessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(0.9999, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(0.05, testWalletDatabaseAccessor.getBalance("Client4", "EUR"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.0001, swap.Amount1, 0.0)
        Assert.assertEquals("BTC", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(0.05, swap.Amount2, 0.0)
        Assert.assertEquals("EUR", swap.origAsset2)
    }

    @Test
    fun testLimitOrderRounding() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 1121.509, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCEUR", volume = -0.00043722)))

        val limitOrder = testLimitDatabaseAccessor.getLastOrder()
        Assert.assertEquals(1000.0 - 0.00043722, limitOrder.remainingVolume, DELTA)
    }
}