package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
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
    private val testFileDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private val testBackOfficeDatabaseAcessor = TestBackOfficeDatabaseAccessor()
    private val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    private val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    private val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    private val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    private val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    private val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()

    private val applicationSettingsCache = ApplicationSettingsCache(TestSettingsDatabaseAccessor())
    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAcessor))
    private val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testDictionariesDatabaseAccessor))
    private val balancesHolder = BalancesHolder(testWalletDatabaseAccessor, TestPersistenceManager(testWalletDatabaseAccessor), assetsHolder, balanceNotificationQueue, balanceUpdateQueue, applicationSettingsCache)

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

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testCancel() {
        val genericService = GenericLimitOrderService(testFileDatabaseAccessor, assetsHolder,
                assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, applicationSettingsCache)
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