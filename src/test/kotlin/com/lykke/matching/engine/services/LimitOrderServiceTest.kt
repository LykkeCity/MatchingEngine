package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
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
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LimitOrderServiceTest {

    var testDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    var testMarketDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    val testWalletDatabaseAcessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAcessor, 60000))
    val balancesHolder = BalancesHolder(testWalletDatabaseAcessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, setOf("Client3"))

    @Before
    fun setUp() {
        testDatabaseAccessor = TestLimitOrderDatabaseAccessor()
        testWalletDatabaseAcessor.clear()

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2, "USD"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2, "EUR"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8, "BTC", 0.0000273))

        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5, 5))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8, 8))

        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testAddLimitOrder() {        
        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 999.9, volume = 1.0)))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.price == 999.9 }
        assertNotNull(order)

        assertEquals(1000.0, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(999.9, testWalletDatabaseAcessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testCancelPrevAndAddLimitOrder() {
        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 100.0, volume = 1.0), uid = 1))
        assertEquals(100.0, testWalletDatabaseAcessor.getReservedBalance("Client1", "USD"))
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 200.0, volume = 1.0), uid = 2))
        assertEquals(300.0, testWalletDatabaseAcessor.getReservedBalance("Client1", "USD"))
        assertEquals(2, testDatabaseAccessor.orders.size)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 300.0, volume = 2.0), true, uid = 3))
        assertEquals(600.0, testWalletDatabaseAcessor.getReservedBalance("Client1", "USD"))
        assertEquals(1, testDatabaseAccessor.orders.size)
        var order = testDatabaseAccessor.loadLimitOrders().find { it.price == 300.0 }
        assertNotNull(order)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 500.0, volume = 1.5), true, uid = 3))
        assertEquals(750.0, testWalletDatabaseAcessor.getReservedBalance("Client1", "USD"))
        assertEquals(1, testDatabaseAccessor.orders.size)
        order = testDatabaseAccessor.loadLimitOrders().find { it.price == 500.0 }
        assertNotNull(order)
    }

    @Test
    fun testNegativeSpread() {
        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, setOf("EUR"), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 100.0, volume = 1.0)))
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 200.0, volume = 1.0)))
        assertEquals(2, testDatabaseAccessor.orders.size)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 300.0, volume = -1.0)))
        assertEquals(3, testDatabaseAccessor.orders.size)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 150.0, volume = -1.0)))
        assertEquals(4, testDatabaseAccessor.orders.size)
    }

    @Test
    fun testBalanceCheck() {
        val service = GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)

        assertTrue { service.isEnoughFunds(buildLimitOrder(price = 2.0, volume = -1000.0), 1000.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder( price = 2.0, volume = -1001.0), 1001.0) }

        assertTrue { service.isEnoughFunds(buildLimitOrder(clientId = "Client2", price = 2.0, volume = 500.0), 500.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder(clientId = "Client2", price = 2.0, volume = 501.0), 501.0) }
    }

    @Test
    fun testAddAndMatchLimitOrder() {
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.512, volume = -10.0, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.524, volume = -10.0, clientId = "Client3"))

        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 122.52, volume = 1.0)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(877.48, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAcessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddAndMatchLimitSellDustOrder() {
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3583.081, volume = 0.00746488, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3581.391, volume = 0.00253512, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3579.183, volume = 0.00253512, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3578.183, volume = 0.00253512, clientId = "Client3"))

        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", price = 3575.782, volume = -0.01)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(3, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(0.0, result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[2].order.status)

        assertEquals(1035.81, testWalletDatabaseAcessor.getBalance("Client1", "EUR"))
        assertEquals(999.99, testWalletDatabaseAcessor.getBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAcessor.getReservedBalance("Client1", "BTC"))
    }

    @Test
    fun testAddAndMatchBuyLimitDustOrder() {
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 4000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3827.395, volume = -0.00703833, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3830.926, volume = -0.01356452, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3832.433, volume = -0.02174805, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3836.76, volume = -0.02740016, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3838.624, volume = -0.03649953, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3842.751, volume = -0.03705699, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3845.948, volume = -0.04872587, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3847.942, volume = -0.05056858, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3851.385, volume = -0.05842735, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3855.364, volume = -0.07678406, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3858.021, volume = -0.07206853, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3861.283, volume = -0.05011803, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3863.035, volume = -0.1, clientId = "Client3"))

        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCEUR", price = 3890.0, volume = 0.5)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(13, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(0.0, result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[2].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[3].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[4].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[5].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[6].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[7].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[8].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[9].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[10].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[11].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[12].order.status)

        assertEquals(2075.46, testWalletDatabaseAcessor.getBalance("Client1", "EUR"))
        assertEquals(0.5, testWalletDatabaseAcessor.getBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAcessor.getReservedBalance("Client1", "BTC"))

        assertEquals(1924.54, testWalletDatabaseAcessor.getBalance("Client3", "EUR"))
        assertEquals(999.5, testWalletDatabaseAcessor.getBalance("Client3", "BTC"))
        assertEquals(0.0, testWalletDatabaseAcessor.getReservedBalance("Client3", "BTC"))
    }

    @Test
    fun testAddAndPartiallyMatchLimitOrder() {
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 2000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 2000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 2000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 2000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.512, volume = -10.0, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.524, volume = -10.0, clientId = "Client3"))

        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 122.52, volume = 11.0)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(1.0, result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)

        assertEquals(774.88, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(122.52, testWalletDatabaseAcessor.getReservedBalance("Client1", "USD"))
    }
}