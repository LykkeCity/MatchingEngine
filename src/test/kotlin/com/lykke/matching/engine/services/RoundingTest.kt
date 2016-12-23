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
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.outgoing.JsonSerializable
import com.lykke.matching.engine.queue.transaction.Swap
import com.lykke.matching.engine.queue.transaction.Transaction
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue

class RoundingTest {
    var testDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    var testLimitDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    var testWalletDatabaseAcessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAcessor = TestBackOfficeDatabaseAccessor()
    val transactionQueue = LinkedBlockingQueue<Transaction>()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val orderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val walletCredentialsCache = WalletCredentialsCache(testBackOfficeDatabaseAcessor)

    val DELTA = 1e-9

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testLimitDatabaseAccessor.clear()
        testWalletDatabaseAcessor.clear()
        transactionQueue.clear()
        tradesInfoQueue.clear()

        testBackOfficeDatabaseAcessor.addAsset(Asset("EUR", 2, "EUR"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("USD", 2, "USD"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("JPY", 2, "JPY"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("BTC", 8, "BTC"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("LKK", 0, "LKK"))
        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Client3", "Client3-Multisig"))
        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Client4", "Client4-Multisig"))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 3, 6))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 3, 8))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 3, 8))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTCLKK", "BTC", "LKK", 2, 8))

        this.walletCredentialsCache.reloadCache()
    }

    @After
    fun tearDown() {
    }

    @Test
    fun testStraightBuy() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(999.0, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.12, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1498.88, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

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
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(1.0, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(998.89, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1.11, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

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
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(999.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.9, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)
        Assert.assertEquals(1499.1, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)

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
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(999.11, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.89, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(1.0, swap.Amount1, 0.0)
        Assert.assertEquals("USD", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(0.89, swap.Amount2, 0.0)
        Assert.assertEquals("EUR", swap.origAsset2)
    }

    @Test
    fun testStraightBuyBTC() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 678.229, volume = -1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(999.0, testWalletDatabaseAcessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(678.23, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAcessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(821.77, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

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
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(1.0, testWalletDatabaseAcessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(321.78, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAcessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(678.22, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

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
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(999.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.00147443, testWalletDatabaseAcessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)
        Assert.assertEquals(1499.99852557, testWalletDatabaseAcessor.getBalance("Client4", "BTC"), DELTA)

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
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(999.99852558, testWalletDatabaseAcessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(1.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.00147442, testWalletDatabaseAcessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(1499.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

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
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "LKK", 50800.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCLKK", volume = -50800.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(14945.59, marketOrder.price!!, DELTA)

        Assert.assertEquals(50800.0, testWalletDatabaseAcessor.getBalance("Client3", "LKK"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAcessor.getBalance("Client4", "LKK"), DELTA)
    }

    @Test
    fun testNotStraightBuyEURJPY() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURJPY", price = 116.356, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "JPY", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 0.00999999999999999))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURJPY", volume = 1.16, straight = false)))
        
        Assert.assertEquals(NotEnoughFunds.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testStraightSellBTCEUR() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 597.169, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 1.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, transactionQueue, balanceNotificationQueue)
        val limitOrderService = GenericLimitOrderService(testLimitDatabaseAccessor, cashOperationService, tradesInfoQueue, quotesNotificationQueue)
        val service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, setOf("LKK"))

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

        Assert.assertEquals(0.0001, testWalletDatabaseAcessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(0.95, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        Assert.assertEquals(0.9999, testWalletDatabaseAcessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(0.05, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.0001, swap.Amount1, 0.0)
        Assert.assertEquals("BTC", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(0.05, swap.Amount2, 0.0)
        Assert.assertEquals("EUR", swap.origAsset2)
    }
}