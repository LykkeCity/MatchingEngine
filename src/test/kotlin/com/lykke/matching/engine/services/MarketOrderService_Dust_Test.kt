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
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
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
    var testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    val transactionQueue = LinkedBlockingQueue<Transaction>()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val walletCredentialsCache = WalletCredentialsCache(testBackOfficeDatabaseAccessor)

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAccessor, 60000))
    val balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, emptySet())

    var limitOrderService = GenericLimitOrderService(false, testLimitDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
    var service = MarketOrderService(testBackOfficeDatabaseAccessor, testDatabaseAccessor, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, rabbitSwapQueue)
    val DELTA = 1e-9

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testLimitDatabaseAccessor.clear()
        testWalletDatabaseAccessor.clear()
        transactionQueue.clear()
        tradesInfoQueue.clear()

        testBackOfficeDatabaseAccessor.addWalletCredentials(WalletCredentials("Client1", "Client1-Multisig"))
        testBackOfficeDatabaseAccessor.addWalletCredentials(WalletCredentials("Client2", "Client2-Multisig"))
        testBackOfficeDatabaseAccessor.addWalletCredentials(WalletCredentials("Client3", "Client3-Multisig"))
        testBackOfficeDatabaseAccessor.addWalletCredentials(WalletCredentials("Client4", "Client4-Multisig"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 2, "LKK", 0.0))
        testBackOfficeDatabaseAccessor.addAsset(Asset("SLR", 2, "SLR", 0.0))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2, "EUR", 0.0))
        testBackOfficeDatabaseAccessor.addAsset(Asset("GBP", 2, "GBP", 0.001))
        testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2, "CHF", 0.0))
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2, "USD"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("JPY", 2, "JPY"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8, "BTC", 0.00001))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC1", 8, "BTC1", 0.0000273))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURJPY", "EUR", "JPY", 3, 6))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCLKK", "BTC", "LKK", 6, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTC1USD", "BTC1", "USD", 3, 7))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTC1LKK", "BTC1", "LKK", 6, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("SLRBTC", "SLR", "BTC", 8, 2))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("SLRBTC1", "SLR", "BTC1", 8, 2))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("LKKEUR", "LKK", "EUR", 5, 2))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("LKKGBP", "LKK", "GBP", 5, 2))

        this.walletCredentialsCache.reloadCache()
    }

    @After
    fun tearDown() {
    }

    fun initServices() {
        limitOrderService = GenericLimitOrderService(false, testLimitDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
        service = MarketOrderService(testBackOfficeDatabaseAccessor, testDatabaseAccessor, limitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, rabbitSwapQueue)
    }

    @Test
    fun testDustMatchOneToOne() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", price = 1000.0, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 0.020009))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = -0.02)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(1000.0, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.02000900", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("20.00", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(0.020009, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(1480.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
        Assert.assertEquals(20.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testDustIncorrectBalanceAndDust1() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTC1USD", price = 610.96, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.14441494999999982))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = 88.23, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(610.96, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.14441495", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC1", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("88.23", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(0.14441495, testWalletDatabaseAccessor.getBalance("Client3", "BTC1"), DELTA)
        Assert.assertEquals(1500 - 88.23, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC1"), DELTA)
        Assert.assertEquals(88.23, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testDustIncorrectBalanceAndDust2() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTC1USD", price = 598.916, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.033407))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = 20.0, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(598.916, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals("20.008", marketOrderReport.order.volume.toString())
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.03340700", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC1", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("20.00", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(0.033407, testWalletDatabaseAccessor.getBalance("Client3", "BTC1"), DELTA)
        Assert.assertEquals(1500 - 20.0, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC1"), DELTA)
        Assert.assertEquals(20.0, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testDustIncorrectBalanceAndDust3() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTC1USD", price = 593.644, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.00092519))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = 0.54, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(593.644, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals("0.549", marketOrderReport.order.volume.toString())
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.00092519", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC1", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("0.54", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(0.00092519, testWalletDatabaseAccessor.getBalance("Client3", "BTC1"), DELTA)
        Assert.assertEquals(1500 - 0.54, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC1"), DELTA)
        Assert.assertEquals(0.54, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
    }

    @Test
    fun testDustNotStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1000.0, volume = 500.0, assetId = "BTCUSD", clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 0.02001))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTCUSD", volume = 20.0, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(1000.0, marketOrderReport.order.price!!, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.02001000", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("20.01", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("USD", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)

        Assert.assertEquals(0.02001, testWalletDatabaseAccessor.getBalance("Client3", "BTC"), DELTA)
        Assert.assertEquals(479.99, testWalletDatabaseAccessor.getBalance("Client3", "USD"), DELTA)
        Assert.assertEquals(20.01, testWalletDatabaseAccessor.getBalance("Client4", "USD"), DELTA)
        Assert.assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client4", "BTC"), DELTA)
    }

    @Test
    fun testBuyDustStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1000.0, volume = -500.0, assetId = "BTC1USD", clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC1", 0.02001))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = 0.0000272, straight = true)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Dust.name, marketOrderReport.order.status)
    }

    @Test
    fun test_20170309_01() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 0.0000782, volume = -4000.0, assetId = "SLRBTC1", clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "SLR", 238619.65864945))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.01))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "SLRBTC1", volume = 127.8722, straight = true)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(127.87220039, marketOrderReport.order.volume, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.01000000", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC1", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("127.88", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("SLR", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)
    }

    @Test
    fun test_20170309_02() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 0.0000782, volume = -4000.0, assetId = "SLRBTC1", clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "SLR", 238619.65864945))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.01))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "SLRBTC1", volume = -0.01, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Matched.name, marketOrderReport.order.status)
        Assert.assertEquals(-0.01, marketOrderReport.order.volume, DELTA)
        Assert.assertEquals(1, marketOrderReport.trades.size)

        Assert.assertEquals("0.01000000", marketOrderReport.trades.first().marketVolume)
        Assert.assertEquals("BTC1", marketOrderReport.trades.first().marketAsset)
        Assert.assertEquals("Client4", marketOrderReport.trades.first().marketClientId)
        Assert.assertEquals("127.87", marketOrderReport.trades.first().limitVolume)
        Assert.assertEquals("SLR", marketOrderReport.trades.first().limitAsset)
        Assert.assertEquals("Client3", marketOrderReport.trades.first().limitClientId)
    }

    @Test
    fun testSellDustStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1000.0, volume = 500.0, assetId = "BTC1USD", clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.02001))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1USD", volume = -0.0000272, straight = true)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Dust.name, marketOrderReport.order.status)
    }

    @Test
    fun testBuyDustNotStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 19739.43939992, volume = 500.0, assetId = "BTC1LKK", clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "LKK", 500.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC1", 0.02001))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1LKK", volume = 0.01, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Dust.name, marketOrderReport.order.status)
    }

    @Test
    fun testSellDustNotStraight() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 19739.43939992, volume = -500.0, assetId = "BTC1LKK", clientId = "Client3"))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC1", 0.02001))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "LKK", 500.0))
        initServices()

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "BTC1LKK", volume = -0.01, straight = false)))

        Assert.assertEquals(1, rabbitSwapQueue.size)
        val marketOrderReport = rabbitSwapQueue.poll() as MarketOrderWithTrades
        Assert.assertEquals(OrderStatus.Dust.name, marketOrderReport.order.status)
    }
}