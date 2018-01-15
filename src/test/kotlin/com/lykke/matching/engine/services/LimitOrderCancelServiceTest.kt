package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
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
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertNull

class LimitOrderCancelServiceTest {
    val testFileDatabaseAccessor = TestFileOrderDatabaseAccessor()
    val testWalletDatabaseAcessor = TestWalletDatabaseAccessor()
    val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    val testBackOfficeDatabaseAcessor = TestBackOfficeDatabaseAccessor()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAcessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testDictionariesDatabaseAccessor, 60000))
    val trustedClients = emptySet<String>()
    val balancesHolder = BalancesHolder(testWalletDatabaseAcessor, assetsHolder, balanceNotificationQueue, balanceUpdateQueue, trustedClients)

    @Before
    fun setUp() {
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "5", price = -100.0))
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "3", price = 300.0))
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "6", price = -200.0))
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "7", price = -300.0))
        testFileDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "8", price = -400.0))

        testBackOfficeDatabaseAcessor.addAsset(Asset("USD", 2))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))

        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testCancel() {
        val genericService = GenericLimitOrderService(testFileDatabaseAccessor, assetsHolder,
                assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients)
        val service = LimitOrderCancelService(genericService, limitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder, orderBookQueue, rabbitOrderBookQueue)

        service.processMessage(MessageBuilder.buildLimitOrderCancelWrapper("3"))

        assertEquals(1, orderBookQueue.size)

        val order = testFileDatabaseAccessor.loadLimitOrders().find { it.id == "3" }
        assertNull(order)
        assertEquals(4, testFileDatabaseAccessor.loadLimitOrders().size)
        val previousOrders = genericService.getAllPreviousOrders("Client1", "EURUSD", true)
        assertEquals(4, previousOrders.size)
        assertFalse(previousOrders.any { it.externalId == "3" })
    }
}