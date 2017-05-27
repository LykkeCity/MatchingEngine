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

class MarketOrderService_Dust_Test {
    var testDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    var testLimitDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    var testWalletDatabaseAcessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAcessor = TestBackOfficeDatabaseAccessor()
    val transactionQueue = LinkedBlockingQueue<Transaction>()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val walletCredentialsCache = WalletCredentialsCache(testBackOfficeDatabaseAcessor)

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAcessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAcessor, 60000))
    val balancesHolder = BalancesHolder(testWalletDatabaseAcessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>())

    var limitOrderService = GenericLimitOrderService(false, testLimitDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
    var service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, rabbitSwapQueue, balanceUpdateQueue, false)
    val DELTA = 1e-9

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testLimitDatabaseAccessor.clear()
        testWalletDatabaseAcessor.clear()
        transactionQueue.clear()
        tradesInfoQueue.clear()

        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Client1", "Client1-Multisig"))
        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Client2", "Client2-Multisig"))
        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Client3", "Client3-Multisig"))
        testBackOfficeDatabaseAcessor.addWalletCredentials(WalletCredentials("Client4", "Client4-Multisig"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("LKK", 2, "LKK", 0.0))
        testBackOfficeDatabaseAcessor.addAsset(Asset("SLR", 2, "SLR", 0.0))
        testBackOfficeDatabaseAcessor.addAsset(Asset("EUR", 2, "EUR", 0.0))
        testBackOfficeDatabaseAcessor.addAsset(Asset("GBP", 2, "GBP", 0.001))
        testBackOfficeDatabaseAcessor.addAsset(Asset("CHF", 2, "CHF", 0.0))
        testBackOfficeDatabaseAcessor.addAsset(Asset("USD", 2, "USD"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("JPY", 2, "JPY"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("BTC", 8, "BTC", 0.00001))
        testBackOfficeDatabaseAcessor.addAsset(Asset("BTC1", 8, "BTC1", 0.0000273))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 3, 6))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8, 8))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTCLKK", "BTC", "LKK", 6, 8))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTC1USD", "BTC1", "USD", 3, 7))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTC1LKK", "BTC1", "LKK", 6, 8))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3, 8))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("SLRBTC", "SLR", "BTC", 8, 2))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("SLRBTC1", "SLR", "BTC1", 8, 2))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("LKKEUR", "LKK", "EUR", 5, 2))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("LKKGBP", "LKK", "GBP", 5, 2))

        this.walletCredentialsCache.reloadCache()
    }

    @After
    fun tearDown() {
    }

    fun initServices() {
        limitOrderService = GenericLimitOrderService(false, testLimitDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
        service = MarketOrderService(testBackOfficeDatabaseAcessor, testDatabaseAccessor, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, transactionQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, true, rabbitSwapQueue, balanceUpdateQueue, false)
    }

    @Test
    fun testDustMatchOneToOne() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 1000.0, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 0.020009))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = -0.02)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(1000.0, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(4, testDatabaseAccessor.trades.size)

        Assert.assertEquals(0.020009, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(-20.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-0.020009, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(20.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)

        Assert.assertEquals(0.020009, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" && it.multisig == "Client3-Multisig" }?.volume)
        Assert.assertEquals(-20.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.multisig == "Client3-Multisig" }?.volume)
        Assert.assertEquals(-0.020009, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" && it.multisig == "Client4-Multisig" }?.volume)
        Assert.assertEquals(20.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.multisig == "Client4-Multisig" }?.volume)

        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" && it.volume == 0.020009 }?.addressFrom)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" && it.volume == 0.020009 }?.addressTo)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.volume == -20.0 }?.addressFrom)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.volume == -20.0 }?.addressTo)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" && it.volume == -0.020009 }?.addressFrom)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" && it.volume == -0.020009 }?.addressTo)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.volume == 20.0 }?.addressFrom)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.volume == 20.0 }?.addressTo)

        Assert.assertEquals(0.020009, testWalletDatabaseAcessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(1480.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAcessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(20.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.020009, swap.Amount1, DELTA)
        Assert.assertEquals("BTC", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(20.0, swap.Amount2, DELTA)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testDustIncorrectBalanceAndDust1() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTC1USD", price = 610.96, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.14441494999999982))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = 88.23, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(610.96, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(4, testDatabaseAccessor.trades.size)

        Assert.assertEquals(0.14441494999999982, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" }?.volume)
        Assert.assertEquals(-88.23, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-0.14441494999999982, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" }?.volume)
        Assert.assertEquals(88.23, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)

        Assert.assertEquals(0.14441494999999982, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.multisig == "Client3-Multisig" }?.volume)
        Assert.assertEquals(-88.23, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.multisig == "Client3-Multisig" }?.volume)
        Assert.assertEquals(-0.14441494999999982, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.multisig == "Client4-Multisig" }?.volume)
        Assert.assertEquals(88.23, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.multisig == "Client4-Multisig" }?.volume)

        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.volume == 0.14441494999999982 }?.addressFrom)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.volume == 0.14441494999999982 }?.addressTo)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.volume == -88.23 }?.addressFrom)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.volume == -88.23 }?.addressTo)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.volume == -0.14441494999999982 }?.addressFrom)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.volume == -0.14441494999999982 }?.addressTo)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.volume == 88.23 }?.addressFrom)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.volume == 88.23 }?.addressTo)

        Assert.assertEquals(0.14441494999999982, testWalletDatabaseAcessor.getBalance("Client3", "BTC1"), DELTA)
        Assert.assertEquals(1500 - 88.23, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAcessor.getBalance("Client4", "BTC1"), DELTA)
        Assert.assertEquals(88.23, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.14441494999999982, swap.Amount1, DELTA)
        Assert.assertEquals("BTC1", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(88.23, swap.Amount2, DELTA)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testDustIncorrectBalanceAndDust2() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTC1USD", price = 598.916, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.033407))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = 20.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(598.916, marketOrder.price!!, DELTA)
        Assert.assertEquals("20.008", marketOrder.volume.toString())
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(4, testDatabaseAccessor.trades.size)

        Assert.assertEquals(0.033407, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" }?.volume)
        Assert.assertEquals(-20.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-0.033407, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" }?.volume)
        Assert.assertEquals(20.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)

        Assert.assertEquals(0.033407, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.multisig == "Client3-Multisig" }?.volume)
        Assert.assertEquals(-20.0, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.multisig == "Client3-Multisig" }?.volume)
        Assert.assertEquals(-0.033407, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.multisig == "Client4-Multisig" }?.volume)
        Assert.assertEquals(20.0, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.multisig == "Client4-Multisig" }?.volume)

        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.volume == 0.033407 }?.addressFrom)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.volume == 0.033407 }?.addressTo)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.volume == -20.0 }?.addressFrom)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.volume == -20.0 }?.addressTo)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.volume == -0.033407 }?.addressFrom)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.volume == -0.033407 }?.addressTo)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.volume == 20.0 }?.addressFrom)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.volume == 20.0 }?.addressTo)

        Assert.assertEquals(0.033407, testWalletDatabaseAcessor.getBalance("Client3", "BTC1"), DELTA)
        Assert.assertEquals(1500 - 20.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAcessor.getBalance("Client4", "BTC1"), DELTA)
        Assert.assertEquals(20.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.033407, swap.Amount1, DELTA)
        Assert.assertEquals("BTC1", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(20.0, swap.Amount2, DELTA)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testDustIncorrectBalanceAndDust3() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTC1USD", price = 593.644, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.00092519))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = 0.54, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals("593.644", marketOrder.price.toString())
        Assert.assertEquals("0.549", marketOrder.volume.toString())
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(4, testDatabaseAccessor.trades.size)

        Assert.assertEquals(0.00092519, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" }?.volume)
        Assert.assertEquals(-0.54, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(-0.00092519, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" }?.volume)
        Assert.assertEquals(0.54, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)

        Assert.assertEquals(0.00092519, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.multisig == "Client3-Multisig" }?.volume)
        Assert.assertEquals(-0.54, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.multisig == "Client3-Multisig" }?.volume)
        Assert.assertEquals(-0.00092519, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.multisig == "Client4-Multisig" }?.volume)
        Assert.assertEquals(0.54, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.multisig == "Client4-Multisig" }?.volume)

        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.volume == 0.00092519 }?.addressFrom)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC1" && it.volume == 0.00092519 }?.addressTo)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.volume == -0.54 }?.addressFrom)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" && it.volume == -0.54 }?.addressTo)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.volume == -0.00092519 }?.addressFrom)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC1" && it.volume == -0.00092519 }?.addressTo)
        Assert.assertEquals("Client3-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.volume == 0.54 }?.addressFrom)
        Assert.assertEquals("Client4-Multisig", testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" && it.volume == 0.54 }?.addressTo)

        Assert.assertEquals(0.00092519, testWalletDatabaseAcessor.getBalance("Client3", "BTC1"), DELTA)
        Assert.assertEquals(1500 - 0.54, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAcessor.getBalance("Client4", "BTC1"), DELTA)
        Assert.assertEquals(0.54, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.00092519, swap.Amount1, DELTA)
        Assert.assertEquals("BTC1", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(0.54, swap.Amount2, DELTA)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testDustNotStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1000.0, volume = 500.0, assetId = "BTCUSD", clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 0.02001))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = 20.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.first()
        Assert.assertEquals(OrderStatus.Matched.name, marketOrder.status)
        Assert.assertEquals(1000.0, marketOrder.price!!, DELTA)
        Assert.assertEquals(1, testDatabaseAccessor.matchingData.filter { it.masterOrderId == marketOrder.id }.size)
        Assert.assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        Assert.assertEquals(1, testLimitDatabaseAccessor.orders.size)
        Assert.assertEquals(0, testLimitDatabaseAccessor.ordersDone.size)

        Assert.assertEquals(-20.01, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "USD" }?.volume)
        Assert.assertEquals(0.02001, testDatabaseAccessor.trades.find { it.clientId == "Client3" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(-0.02001, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "BTC" }?.volume)
        Assert.assertEquals(20.01, testDatabaseAccessor.trades.find { it.clientId == "Client4" && it.assetId == "USD" }?.volume)

        Assert.assertEquals(0.02001, testWalletDatabaseAcessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(479.99, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(20.01, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAcessor.getBalance("Client4", "BTC"), DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.02001, swap.Amount1, DELTA)
        Assert.assertEquals("BTC", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(20.01, swap.Amount2, DELTA)
        Assert.assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testBuyDustStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1000.0, volume = -500.0, assetId = "BTC1USD", clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "BTC1", 0.02001))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = 0.0000272, straight = true)))

        Assert.assertEquals(OrderStatus.Dust.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun test_20170309_01() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 0.0000782, volume = -4000.0, assetId = "SLRBTC1", clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "SLR", 238619.65864945))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.01))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "SLRBTC1", volume = 127.8722, straight = true)))

        Assert.assertEquals(OrderStatus.Matched.name, testDatabaseAccessor.getLastOrder().status)
        Assert.assertEquals(127.87220039, testDatabaseAccessor.getLastOrder().volume, DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.01, swap.Amount1, DELTA)
        Assert.assertEquals("BTC1", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(127.88, swap.Amount2, DELTA)
        Assert.assertEquals("SLR", swap.origAsset2)
    }

    @Test
    fun test_20170309_02() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 0.0000782, volume = -4000.0, assetId = "SLRBTC1", clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "SLR", 238619.65864945))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.01))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "SLRBTC1", volume = -0.01, straight = false)))

        Assert.assertEquals(OrderStatus.Matched.name, testDatabaseAccessor.getLastOrder().status)
        Assert.assertEquals(-0.01, testDatabaseAccessor.getLastOrder().volume, DELTA)

        val swap = transactionQueue.take() as Swap
        Assert.assertEquals("Client4", swap.clientId1)
        Assert.assertEquals(0.01, swap.Amount1, DELTA)
        Assert.assertEquals("BTC1", swap.origAsset1)
        Assert.assertEquals("Client3", swap.clientId2)
        Assert.assertEquals(127.87, swap.Amount2, DELTA)
        Assert.assertEquals("SLR", swap.origAsset2)
    }

    @Test
    fun testSellDustStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1000.0, volume = 500.0, assetId = "BTC1USD", clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.02001))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = -0.0000272, straight = true)))

        Assert.assertEquals(OrderStatus.Dust.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testBuyDustNotStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 19739.43939992, volume = 500.0, assetId = "BTC1LKK", clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "LKK", 500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.02001))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1LKK", volume = 0.01, straight = false)))

        Assert.assertEquals(OrderStatus.Dust.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testSellDustNotStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 19739.43939992, volume = -500.0, assetId = "BTC1LKK", clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "BTC1", 0.02001))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "LKK", 500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1LKK", volume = -0.01, straight = false)))

        Assert.assertEquals(OrderStatus.Dust.name, testDatabaseAccessor.getLastOrder().status)
    }
}