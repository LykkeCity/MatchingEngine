package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.TestLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus
import org.junit.Before
import org.junit.Test
import java.util.Date
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LimitOrderServiceTest {

    val testDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    val testWalletDatabaseAcessor = TestWalletDatabaseAccessor()

    @Before
    fun setUp() {
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(rowKey = "1", price = 100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(rowKey = "2", price = 200.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(rowKey = "3", price = 300.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(rowKey = "4", price = 400.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(rowKey = "5", price = -100.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(rowKey = "6", price = -200.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(rowKey = "7", price = -300.0))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(rowKey = "8", price = -400.0))

        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "CHF"))

        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testAddLimitOrder() {
        val service = LimitOrderService(testDatabaseAccessor, CashOperationService(testWalletDatabaseAcessor))
        service.processMessage(buildLimitOrderWrapper(buildLimitOrder(price = 999.9)))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.price == 999.9 }
        assertNotNull(order)
    }

    @Test
    fun testBalanceCheck() {
        val service = LimitOrderService(testDatabaseAccessor, CashOperationService(testWalletDatabaseAcessor))

        assertTrue { service.isEnoughFunds(buildLimitOrder(price = 2.0, volume = -1000.0), 1000.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder( price = 2.0, volume = -1001.0), 1001.0) }

        assertTrue { service.isEnoughFunds(buildLimitOrder(clientId = "Client2", price = 2.0, volume = 500.0), 500.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder(clientId = "Client2", price = 2.0, volume = 501.0), 501.0) }
    }

    private fun buildLimitOrderWrapper(order: LimitOrder): MessageWrapper {
        return MessageWrapper(MessageType.LIMIT_ORDER, ProtocolMessages.LimitOrder.newBuilder()
                .setUid(Date().time)
                .setTimestamp(order.createdAt.time)
                .setClientId(order.clientId)
                .setAssetPairId(order.assetPairId)
                .setVolume(order.volume)
                .setPrice(order.price).build().toByteArray(), null)
    }
}

fun buildLimitOrder(rowKey: String = Date().time.toString(),
                    assetId: String = "EURUSD",
                    clientId: String = "Client1",
                    lastMatchTime: Date? = null,
                    price: Double = 100.0,
                    registered: Date = Date(),
                    status: String = OrderStatus.InOrderBook.name,
                    volume:Double = 1000.0): LimitOrder =
        LimitOrder(
                uid = rowKey,
                assetPairId = assetId,
                clientId = clientId,
                lastMatchTime = lastMatchTime,
                price = price,
                createdAt = registered,
                registered = Date(),
                status = status,
                volume = volume,
                remainingVolume = volume,
                matchedOrders = null
        )
