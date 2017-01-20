package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.queue.transaction.Transaction
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
        val service = LimitOrderCancelService(GenericLimitOrderService(testDatabaseAccessor, CashOperationService(testWalletDatabaseAcessor, testBackOfficeDatabaseAcessor, LinkedBlockingQueue<Transaction>(), balanceNotificationQueue), tradesInfoQueue, quotesNotificationQueue))
        service.processMessage(buildLimitOrderCancelWrapper("3"))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.id == "3" }
        assertNull(order)
        assertEquals(7, testDatabaseAccessor.loadLimitOrders().size)
    }

    private fun buildLimitOrderCancelWrapper(uid: String): MessageWrapper {
        return MessageWrapper("Test", MessageType.LIMIT_ORDER_CANCEL.type, ProtocolMessages.LimitOrderCancel.newBuilder()
                .setUid(Date().time).setLimitOrderId(uid.toLong()).build().toByteArray(), null)
    }
}