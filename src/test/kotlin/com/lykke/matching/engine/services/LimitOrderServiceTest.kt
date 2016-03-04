package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.database.TestLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderSide
import com.lykke.matching.engine.order.OrderSide.Buy
import com.lykke.matching.engine.order.OrderSide.Sell
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
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", rowKey = "1", price = 100.0, orderType = Buy.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", rowKey = "2", price = 200.0, orderType = Buy.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", rowKey = "3", price = 300.0, orderType = Buy.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", rowKey = "4", price = 400.0, orderType = Buy.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Sell", rowKey = "5", price = 100.0, orderType = Sell.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Sell", rowKey = "6", price = 200.0, orderType = Sell.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Sell", rowKey = "7", price = 300.0, orderType = Sell.name))
        testDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Sell", rowKey = "8", price = 400.0, orderType = Sell.name))

        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "CHF"))

        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
    }

    @Test
    fun testAddLimitOrder() {
        val service = LimitOrderService(testDatabaseAccessor, CashOperationService(testWalletDatabaseAcessor))
        service.processMessage(buildByteArray(buildLimitOrder(price = 999.9)))

        val order = testDatabaseAccessor.loadLimitOrders().find { it.price == 999.9 }
        assertNotNull(order)
    }

    @Test
    fun testBalanceCheck() {
        val service = LimitOrderService(testDatabaseAccessor, CashOperationService(testWalletDatabaseAcessor))

        assertTrue { service.isEnoughFunds(buildLimitOrder(partitionKey = "EURUSD_Sell", orderType = Sell.name, price = 2.0, volume = 1000.0), 1000.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder(partitionKey = "EURUSD_Sell", orderType = Sell.name, price = 2.0, volume = 1001.0), 1001.0) }

        assertTrue { service.isEnoughFunds(buildLimitOrder(partitionKey = "EURUSD_Buy", clientId = "Client2", orderType = Buy.name, price = 2.0, volume = 500.0), 500.0) }
        assertFalse { service.isEnoughFunds(buildLimitOrder(partitionKey = "EURUSD_Buy", clientId = "Client2", orderType = Buy.name, price = 2.0, volume = 501.0), 501.0) }
    }

    private fun buildByteArray(order: LimitOrder): ByteArray {
        return ProtocolMessages.LimitOrder.newBuilder()
                .setUid(order.rowKey.toLong())
                .setTimestamp(order.createdAt.time)
                .setClientId(order.clientId)
                .setAssetId(order.assetPair)
                .setOrderAction(OrderSide.valueOf(order.orderType).side)
                .setBlockChain(order.blockChain)
                .setVolume(order.volume)
                .setPrice(order.price).build().toByteArray()
    }
}

fun buildLimitOrder(partitionKey: String = "EURUSD_Buy",
                    rowKey: String = "1",
                    assetId: String = "EURUSD",
                    clientId: String = "Client1",
                    lastMatchTime: Date? = null,
                    orderType: String = Buy.name,
                    price: Double = 100.0,
                    registered: Date = Date(),
                    status: String = OrderStatus.InOrderBook.name,
                    volume:Double = 1000.0): LimitOrder =
        LimitOrder(
                partitionKey = partitionKey,
                rowKey = rowKey,
                assetId = assetId,
                clientId = clientId,
                lastMatchTime = lastMatchTime,
                orderType = orderType,
                blockChain = "",
                price = price,
                createdAt = registered,
                registered = Date(),
                status = status,
                volume = volume,
                remainingVolume = volume,
                matchedOrders = null
        )
