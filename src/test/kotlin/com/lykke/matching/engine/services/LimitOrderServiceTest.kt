package com.lykke.matching.engine.services

import com.lykke.matching.engine.cache.WalletCredentialsCache
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
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
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.RoundingUtils
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LimitOrderServiceTest {

    private var testDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private var testMarketDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    private val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    private val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    private val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    private val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    private val walletCredentialsCache = WalletCredentialsCache(testBackOfficeDatabaseAccessor)
    private val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()

    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    private val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAccessor, 60000))
    private val balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, setOf("Client3"))

    @Before
    fun setUp() {
        testDatabaseAccessor = TestFileOrderDatabaseAccessor()
        testWalletDatabaseAccessor.clear()

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2, "USD"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2, "EUR"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("ETH", 6, "ETH"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8, "BTC"))

        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5, 5))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8, 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("ETHBTC", "ETH", "BTC", 5, 5))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testAddLimitOrder() {        
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 999.9, volume = 1.0)))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.price == 999.9 }
        assertNotNull(order)

        assertEquals(1000.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(999.9, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddSellLimitOrder() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 999.9, volume = -1.0)))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.price == 999.9 }
        assertNotNull(order)

        assertEquals(1000.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(1.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))
    }

    @Test
    fun testCancelPrevAndAddLimitOrder() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 100.0, volume = 1.0, uid = "1")))
        assertEquals(100.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 200.0, volume = 1.0, uid = "2")))
        assertEquals(300.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(2, testDatabaseAccessor.getOrders("EURUSD", true).size)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 300.0, volume = 2.0, uid = "3"), true))
        assertEquals(600.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(1, testDatabaseAccessor.getOrders("EURUSD", true).size)
        var order = testDatabaseAccessor.loadLimitOrders().find { it.price == 300.0 }
        assertNotNull(order)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 500.0, volume = 1.5, uid = "3"), true))
        assertEquals(750.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(1, testDatabaseAccessor.getOrders("EURUSD", true).size)
        order = testDatabaseAccessor.loadLimitOrders().find { it.price == 500.0 }
        assertNotNull(order)
    }

    @Test
    fun testRestartAndCancel() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 100.0, volume = 1.0, uid = "1")))
        assertEquals(100.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 200.0, volume = 1.0, uid = "2")))
        assertEquals(300.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(2, testDatabaseAccessor.getOrders("EURUSD", true).size)

        val genericService = GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)

        assertNotNull(genericService.getAllPreviousOrders("Client1", "EURUSD", true).find { it.externalId == "2" })
        val cancelService = LimitOrderCancelService(genericService, limitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder)
        cancelService.processMessage(MessageBuilder.buildLimitOrderCancelWrapper("2"))
    }

    @Test
    fun testNegativeSpread() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, setOf("EUR"), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 100.0, volume = 1.0)))
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 200.0, volume = 1.0)))
        assertEquals(2, testDatabaseAccessor.getOrders("EURUSD", true).size)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 300.0, volume = -1.0)))
        assertEquals(1, testDatabaseAccessor.getOrders("EURUSD", false).size)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 150.0, volume = -1.0)))
        assertEquals(1, testDatabaseAccessor.getOrders("EURUSD", false).size)
    }

    @Test
    fun testBalanceCheck() {
        val service = GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)

        assertTrue { service.isEnoughFunds(buildLimitOrder(price = 2.0, volume = -1000.0), 1000.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder( price = 2.0, volume = -1001.0), 1001.0) }

        assertTrue { service.isEnoughFunds(buildLimitOrder(clientId = "Client2", price = 2.0, volume = 500.0), 500.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder(clientId = "Client2", price = 2.0, volume = 501.0), 501.0) }
    }

    @Test
    fun testAddAndMatchLimitOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.512, volume = -10.0, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.524, volume = -10.0, clientId = "Client3"))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 122.52, volume = 1.0)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(877.48, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderRounding() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 4199.351, volume = 0.00357198)))

        assertEquals(1, limitOrdersQueue.size)
        var result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        assertEquals(15.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", price = 4199.351, volume = -0.00357198)))

        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)

        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddLimitOrderEURUSD() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", price = 325.7152, volume = 0.046053)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        assertEquals(15.01, testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", clientId = "Client1", price = 3200.0, volume = -0.01)))
        assertEquals(1, limitOrdersQueue.size)
        var result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", clientId = "Client3", price = 3200.0, volume = 0.009973)))
        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(1000 - 0.009973, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(1031.92, testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(0.009973, testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(968.08, testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust1() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", clientId = "Client1", price = 3200.0, volume = -0.01)))
        assertEquals(1, limitOrdersQueue.size)
        var result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", clientId = "Client3", price = 3200.0, volume = 0.01002635)))
        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(0.00002635, result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)

        assertEquals(999.99, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(1032.0, testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(0.01, testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(968.0, testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
    }

    @Test
    fun testAddAndMatchBuyLimitOrderWithDust() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", clientId = "Client1", price = 3200.0, volume = 0.01)))
        assertEquals(1, limitOrdersQueue.size)
        var result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)
        assertEquals(32.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", clientId = "Client3", price = 3200.0, volume = -0.009973)))
        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(1000 - 0.009973, testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(31.91, testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(0.009973, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(968.09, testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(0.09, testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithSamePrice() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.512, volume = -10.0, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.524, volume = -10.0, clientId = "Client3"))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 122.512, volume = 1.0)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(877.48, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddAndMatchLimitSellDustOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3583.081, volume = 0.00746488, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3581.391, volume = 0.00253512, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3579.183, volume = 0.00253512, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "BTCEUR", price = 3578.183, volume = 0.00253512, clientId = "Client3"))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", price = 3575.782, volume = -0.01)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(3, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(0.0, result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[2].order.status)

        assertEquals(1035.81, testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(999.99, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
    }

    @Test
    fun testAddAndMatchBuyLimitDustOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 4000.0))
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

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
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

        assertEquals(2075.46, testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(0.5, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(1924.54, testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(999.5, testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client3", "BTC"))
    }

    @Test
    fun testAddAndPartiallyMatchLimitOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 2000.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.512, volume = -10.0, clientId = "Client3"))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(assetId = "EURUSD", price = 122.524, volume = -10.0, clientId = "Client3"))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 122.52, volume = 11.0)))

        assertEquals(1, limitOrdersQueue.size)
        val result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(1.0, result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)

        assertEquals(774.88, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(122.52, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddMatchWithMarketOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 2000.0))

        val genericService = GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
        val service = SingleLimitOrderService(genericService, limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 122.52, volume = 11.0)))

        assertEquals(1, limitOrdersQueue.size)
        var result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)
        assertEquals(11.0, result.orders[0].order.remainingVolume)

        assertEquals(2000.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(1347.72, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        val marketService = MarketOrderService(testBackOfficeDatabaseAccessor, testMarketDatabaseAccessor, genericService, assetsHolder, assetsPairsHolder, balancesHolder, limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, walletCredentialsCache, rabbitSwapQueue)
        marketService.processMessage(MessageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client3", assetId = "EURUSD", volume = -10.0)))

        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(1.0, result.orders[0].order.remainingVolume)

        assertEquals(774.8, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(122.52, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddAndMatchWithLimitOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "BTC", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 406.24, reservedBalance = 263.33))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 2000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 2000.0))

        val genericService = GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
        val service = SingleLimitOrderService(genericService, limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", price = 4421.0, volume = 	-0.00045239)))

        assertEquals(1, limitOrdersQueue.size)
        var result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)
        assertEquals(-0.00045239, result.orders[0].order.remainingVolume)

        assertEquals(2000.0, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(0.00045239, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"))

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client4", assetId = "BTCUSD", price = 4425.0, volume = 0.032)))

        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(0.03154761, result.orders[0].order.remainingVolume)
        assertEquals(139.6, result.orders[0].order.reservedLimitVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(0.0, result.orders[1].order.remainingVolume)

        assertEquals(1999.99954761, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(2000.00045239, testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(RoundingUtils.round(263.33 + 139.6, 2, true), testWalletDatabaseAccessor.getReservedBalance("Client4", "USD"))
    }

    @Test
    fun testAddAndMatchAndCancel() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1.00148747))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "ETH", 1000.0))

        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "ETHBTC", uid = "1", price = 0.07963, volume = 2.244418)))

        assertEquals(1, limitOrdersQueue.size)
        var result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        assertEquals(1.00148747, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(0.17872301, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "ETHBTC", uid = "2", price = 0.07948, volume = -0.01462)))
        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "ETHBTC", uid = "3", price = 0.07954, volume = -0.031344)))
        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2",assetId = "ETHBTC",  uid = "4", price = 0.07958, volume = -0.041938)))
        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(0.99448784, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(0.17172338, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "ETHBTC", uid = "5", price = 0.07948, volume = -0.000001)))
        assertEquals(1, limitOrdersQueue.size)
        result = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(0.99448777, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(0.17172331, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        val genericService = GenericLimitOrderService(testDatabaseAccessor, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)

        val cancelService = LimitOrderCancelService(genericService, limitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder)
        cancelService.processMessage(MessageBuilder.buildLimitOrderCancelWrapper("1"))

        assertEquals(0.99448777, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
    }
}