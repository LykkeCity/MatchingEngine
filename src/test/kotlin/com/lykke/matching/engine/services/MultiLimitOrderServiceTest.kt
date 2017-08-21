package com.lykke.matching.engine.services

import com.lykke.matching.engine.cache.WalletCredentialsCache
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
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.VolumePrice
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals

class MultiLimitOrderServiceTest {
    var testDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    var testMarketDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val trustedLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val walletCredentialsCache = WalletCredentialsCache(testBackOfficeDatabaseAccessor)
    val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()


    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAccessor, 60000))
    val balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, setOf("Client1"))
    var genericLimitService = initLimitService()
    var service = initService()

    @Before
    fun setUp() {
        testDatabaseAccessor = TestLimitOrderDatabaseAccessor()
        testWalletDatabaseAccessor.clear()

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2, "USD"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2, "EUR"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2, "CHF"))

        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5, 5))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))

        genericLimitService = initLimitService()
        service = initService()
    }

    @Test
    fun testAddLimitOrder() {
        service.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(100.0, 1.2), VolumePrice(100.0, 1.3))))

        assertEquals(1000.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(1, limitOrdersQueue.size)
        val limitOrders = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(1.2, limitOrders.orders[0].order.price)
        assertEquals(1.3, limitOrders.orders[1].order.price)
    }

    @Test
    fun testAdd2LimitOrder() {
        service.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(100.0, 1.2), VolumePrice(100.0, 1.3))))

        assertEquals(1000.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(1, limitOrdersQueue.size)
        var limitOrders = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(1.2, limitOrders.orders[0].order.price)
        assertEquals(1.3, limitOrders.orders[1].order.price)


        service.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(100.0, 1.4), VolumePrice(100.0, 1.5))))

        assertEquals(1, limitOrdersQueue.size)
        limitOrders = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(1.4, limitOrders.orders[0].order.price)
        assertEquals(1.5, limitOrders.orders[1].order.price)
    }

    @Test
    fun testAddAndCancelLimitOrder() {
        service.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(100.0, 1.2), VolumePrice(100.0, 1.3))))

        assertEquals(1000.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(1, limitOrdersQueue.size)
        var limitOrders = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(1.2, limitOrders.orders[0].order.price)
        assertEquals(1.3, limitOrders.orders[1].order.price)


        service.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(100.0, 1.4), VolumePrice(100.0, 1.5))))

        assertEquals(1, limitOrdersQueue.size)
        limitOrders = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(1.4, limitOrders.orders[0].order.price)
        assertEquals(1.5, limitOrders.orders[1].order.price)


        service.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(100.0, 2.0), VolumePrice(100.0, 2.1)), cancel = true))

        assertEquals(1, limitOrdersQueue.size)
        limitOrders = limitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(6, limitOrders.orders.size)
        assertEquals(1.2, limitOrders.orders[0].order.price)
        assertEquals(1.3, limitOrders.orders[1].order.price)
        assertEquals(1.4, limitOrders.orders[2].order.price)
        assertEquals(1.5, limitOrders.orders[3].order.price)
        assertEquals(2.0, limitOrders.orders[4].order.price)
        assertEquals(2.1, limitOrders.orders[5].order.price)
    }

    @Test
    fun testAddAndMatchLimitOrder() {
        service.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(100.0, 1.3), VolumePrice(100.0, 1.2))))

        assertEquals(1, limitOrdersQueue.size)
        limitOrdersQueue.poll()

        val singleOrderService = initSingleLimitOrderService()
        singleOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", price = 1.25, volume = -150.0)))

        assertEquals(1, trustedLimitOrdersQueue.size)
        var result = trustedLimitOrdersQueue.poll() as LimitOrdersReport

        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(-50.0, result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(1.3, result.orders[1].order.price)

        assertEquals(870.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(1100.0, testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(1130.0, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(900.0, testWalletDatabaseAccessor.getBalance("Client2", "EUR"))
        assertEquals(50.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        service.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", volumes = listOf(VolumePrice(10.0, 1.3), VolumePrice(100.0, 1.26), VolumePrice(100.0, 1.2)), cancel = true))

        assertEquals(1, trustedLimitOrdersQueue.size)
        result = trustedLimitOrdersQueue.poll() as LimitOrdersReport

        assertEquals(3, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(0.0, result.orders[0].order.remainingVolume)
        assertEquals(1.3, result.orders[0].order.price)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(1.25, result.orders[1].order.price)
        assertEquals(OrderStatus.Processing.name, result.orders[2].order.status)
        assertEquals(60.0, result.orders[2].order.remainingVolume)
        assertEquals(1.26, result.orders[2].order.price)

        assertEquals(807.5, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(1150.0, testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(1192.5, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(850.0, testWalletDatabaseAccessor.getBalance("Client2", "EUR"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))
    }

    private fun initLimitService() = GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)
    private fun initSingleLimitOrderService() = SingleLimitOrderService(genericLimitService, trustedLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)
    private fun initService() = MultiLimitOrderService(genericLimitService, limitOrdersQueue, trustedLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, testMarketDatabaseAccessor)

    private fun buildMultiLimitOrderWrapper(pair: String, clientId: String, volumes: List<VolumePrice>, cancel: Boolean = false): MessageWrapper {
        return MessageWrapper("Test", MessageType.MULTI_LIMIT_ORDER.type, buildLimitOrder(pair, clientId, volumes, cancel).toByteArray(), null)
    }

    private fun buildLimitOrder(assetPairId: String, clientId: String, volumes: List<VolumePrice>, cancel: Boolean): ProtocolMessages.OldMultiLimitOrder {
        val uid = Date().time
        val orderBuilder = ProtocolMessages.OldMultiLimitOrder.newBuilder()
                .setUid(uid)
                .setTimestamp(uid)
                .setClientId(clientId)
                .setAssetPairId(assetPairId)
                .setCancelAllPreviousLimitOrders(cancel)
        volumes.forEachIndexed { index, volume ->
            orderBuilder.addOrders(ProtocolMessages.OldMultiLimitOrder.Order.newBuilder()
                    .setVolume(volume.volume)
                    .setPrice(volume.price)
                    .build())
        }
        return orderBuilder.build()
    }
}