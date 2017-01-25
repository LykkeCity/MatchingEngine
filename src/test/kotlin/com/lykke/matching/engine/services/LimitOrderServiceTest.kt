package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestLimitOrderDatabaseAccessor
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
import com.lykke.matching.engine.outgoing.JsonSerializable
import com.lykke.matching.engine.outgoing.OrderBook
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LimitOrderServiceTest {

    val testDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    val testWalletDatabaseAcessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAcessor = TestBackOfficeDatabaseAccessor()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAcessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAcessor, 60000))
    val balancesHolder = BalancesHolder(testWalletDatabaseAcessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>())

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testWalletDatabaseAcessor.clear()

        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5, 5))

        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testAddLimitOrder() {        
        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), orderBookQueue, rabbitOrderBookQueue)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 999.9)))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.price == 999.9 }
        assertNotNull(order)
    }

    @Test
    fun testCancelPrevAndAddLimitOrder() {
        val service = SingleLimitOrderService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), orderBookQueue, rabbitOrderBookQueue)
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 100.0)))
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 200.0)))
        assertEquals(2, testDatabaseAccessor.orders.size)

        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 300.0), true))
        assertEquals(1, testDatabaseAccessor.orders.size)
        val order = testDatabaseAccessor.loadLimitOrders().find { it.price == 300.0 }
        assertNotNull(order)
    }

    @Test
    fun testBalanceCheck() {
        val service = GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""), assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue)

        assertTrue { service.isEnoughFunds(buildLimitOrder(price = 2.0, volume = -1000.0), 1000.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder( price = 2.0, volume = -1001.0), 1001.0) }

        assertTrue { service.isEnoughFunds(buildLimitOrder(clientId = "Client2", price = 2.0, volume = 500.0), 500.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder(clientId = "Client2", price = 2.0, volume = 501.0), 501.0) }
    }

    private fun buildLimitOrderWrapper(order: LimitOrder, cancel: Boolean = false): MessageWrapper {
        return MessageWrapper("Test", MessageType.LIMIT_ORDER.type, ProtocolMessages.LimitOrder.newBuilder()
                .setUid(Date().time)
                .setTimestamp(order.createdAt.time)
                .setClientId(order.clientId)
                .setAssetPairId(order.assetPairId)
                .setVolume(order.volume)
                .setPrice(order.price).setCancelAllPreviousLimitOrders(cancel).build().toByteArray(), null)
    }
}

fun buildLimitOrder(uid: String = UUID.randomUUID().toString(),
                    assetId: String = "EURUSD",
                    clientId: String = "Client1",
                    price: Double = 100.0,
                    registered: Date = Date(),
                    status: String = OrderStatus.InOrderBook.name,
                    volume:Double = 1000.0): LimitOrder =
        LimitOrder(uid, uid, assetId, clientId, volume, price, status, registered, registered, volume, null)
