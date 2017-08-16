package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
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
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertNull

class LimitOrderCancelServiceTest {
    val testDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    val testWalletDatabaseAcessor = TestWalletDatabaseAccessor()
    var testBackOfficeDatabaseAcessor = TestBackOfficeDatabaseAccessor()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val limitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()

    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAcessor, 60000))
    val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAcessor, 60000))
    val balancesHolder = BalancesHolder(testWalletDatabaseAcessor, assetsHolder, balanceNotificationQueue, balanceUpdateQueue)

    @Before
    fun setUp() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "1", price = 100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "2", price = 200.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "3", price = 300.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "4", price = 400.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "5", price = -100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "6", price = -200.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "7", price = -300.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "8", price = -400.0))

        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, 5))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5, 5))

        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testCancel() {
        val service = LimitOrderCancelService(GenericLimitOrderService(false, testDatabaseAccessor, FileOrderBookDatabaseAccessor(""),
                assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue), limitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder)
        service.processMessage(buildLimitOrderCancelWrapper("3"))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.id == "3" }
        assertNull(order)
        assertEquals(7, testDatabaseAccessor.loadLimitOrders().size)
    }

    private fun buildLimitOrderCancelWrapper(uid: String): MessageWrapper {
        return MessageWrapper("Test", MessageType.OLD_LIMIT_ORDER_CANCEL.type, ProtocolMessages.OldLimitOrderCancel.newBuilder()
                .setUid(Date().time).setLimitOrderId(uid.toLong()).build().toByteArray(), null)
    }
}