package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
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
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals

class LimitOrderServiceDustTest {

    var testDatabaseAccessor = TestFileOrderDatabaseAccessor()
    val testWalletDatabaseAcessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val trustedLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAcessor, 60000))
    val trustedClients = setOf("Client3")
    val balancesHolder = BalancesHolder(testWalletDatabaseAcessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, trustedClients)

    @Before
    fun setUp() {
        testDatabaseAccessor = TestFileOrderDatabaseAccessor()
        testWalletDatabaseAcessor.clear()

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

        testWalletDatabaseAcessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust1() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients), limitOrdersQueue, trustedLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = -0.05)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        var result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = 0.04997355)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(1000 - 0.04997355, testWalletDatabaseAcessor.getBalance("Client1", "BTC"))
        assertEquals(1159.92, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(1000 + 0.04997355, testWalletDatabaseAcessor.getBalance("Client2", "BTC"))
        assertEquals(840.08, testWalletDatabaseAcessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust2() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients), limitOrdersQueue, trustedLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = -0.05)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        var result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = 0.05002635)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)

        assertEquals(999.95, testWalletDatabaseAcessor.getBalance("Client1", "BTC"))
        assertEquals(1160.0, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(1000.05, testWalletDatabaseAcessor.getBalance("Client2", "BTC"))
        assertEquals(840.0, testWalletDatabaseAcessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust3() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients), limitOrdersQueue, trustedLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = 0.05)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        var result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = -0.04997355)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(1000 + 0.04997355, testWalletDatabaseAcessor.getBalance("Client1", "BTC"))
        assertEquals(840.09, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(1000 - 0.04997355, testWalletDatabaseAcessor.getBalance("Client2", "BTC"))
        assertEquals(1159.91, testWalletDatabaseAcessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust4() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients), limitOrdersQueue, trustedLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = 0.05)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        var result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = -0.05002635)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)

        assertEquals(1000.05, testWalletDatabaseAcessor.getBalance("Client1", "BTC"))
        assertEquals(840.0, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(999.95, testWalletDatabaseAcessor.getBalance("Client2", "BTC"))
        assertEquals(1160.0, testWalletDatabaseAcessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust5() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients), limitOrdersQueue, trustedLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = -0.05)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        var result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = 0.0499727)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(999.9500273, testWalletDatabaseAcessor.getBalance("Client1", "BTC"))
        assertEquals(1159.92, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(1000.0499727, testWalletDatabaseAcessor.getBalance("Client2", "BTC"))
        assertEquals(840.08, testWalletDatabaseAcessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust6() {
        val service = SingleLimitOrderService(GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients), limitOrdersQueue, trustedLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = 0.05)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        var result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = -0.0499727)))
        assertEquals(1, trustedLimitOrdersQueue.size)
        result = trustedLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(1000.0499727, testWalletDatabaseAcessor.getBalance("Client1", "BTC"))
        assertEquals(840.09, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(999.9500273, testWalletDatabaseAcessor.getBalance("Client2", "BTC"))
        assertEquals(1159.91, testWalletDatabaseAcessor.getBalance("Client2", "USD"))
    }

}