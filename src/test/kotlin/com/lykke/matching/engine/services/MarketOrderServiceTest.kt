package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.database.TestLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderSide
import com.lykke.matching.engine.order.OrderSide.Buy
import com.lykke.matching.engine.order.OrderSide.Sell
import com.lykke.matching.engine.order.OrderStatus.InOrderBook
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.Date
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class MarketOrderServiceTest {
    var testDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    var testLimitDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    var testWalletDatabaseAcessor = TestWalletDatabaseAccessor()

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testLimitDatabaseAccessor.clear()
        testWalletDatabaseAcessor.clear()
    }

    @After
    fun tearDown() {
    }

    @Test
    fun testNoLiqudity() {
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", price = 1.5, volume = 1000.0, clientId = "Client1", orderType = Buy.name))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService)

        service.processMessage(buildByteArray(buildMarketOrder()))
        assertEquals(NoLiquidity.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testNotEnoughFundsClientOrder() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", price = 1.6, volume = 1000.0, clientId = "Client1", orderType = Buy.name))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", price = 1.5, volume = 1000.0, clientId = "Client2", orderType = Buy.name))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService)

        service.processMessage(buildByteArray(buildMarketOrder(clientId = "Client1", assetId = "EURUSD", volume = 1000.0, orderType = Sell.name)))
        assertEquals(NotEnoughFunds.name, testLimitDatabaseAccessor.orders.find { it.price == 1.6 }?.status)
    }

    @Test
    fun testNoLiqudityToFullyFill() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", price = 1.5, volume = 1000.0, clientId = "Client2", orderType = Buy.name))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 2000.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService)

        service.processMessage(buildByteArray(buildMarketOrder(clientId = "Client2", assetId = "EURUSD", volume = 2000.0, orderType = Sell.name)))
        assertEquals(NoLiquidity.name, testDatabaseAccessor.getLastOrder().status)
        assertEquals(InOrderBook.name, testLimitDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testNotEnoughFundsMarketOrder() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", price = 1.5, volume = 1000.0, clientId = "Client3", orderType = Buy.name))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 900.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService)

        service.processMessage(buildByteArray(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1000.0, orderType = Sell.name)))
        assertEquals(NotEnoughFunds.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testMatchOneToOne() {
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", price = 1.5, volume = 1000.0, clientId = "Client3", orderType = Buy.name))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 3000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 2000.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService)

        service.processMessage(buildByteArray(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1000.0, orderType = Sell.name)))

        val marketOrder = testDatabaseAccessor.getLastOrder()
        assertEquals(Matched.name, marketOrder.status)
        assertEquals(1.5, marketOrder.price)
        assertEquals(1, marketOrder.loadMatchedOrdersList().size)

        assertEquals(0, testLimitDatabaseAccessor.orders.size)
        assertEquals(1, testLimitDatabaseAccessor.ordersDone.size)

        val limitOrder = testLimitDatabaseAccessor.ordersDone.first()
        assertEquals(Matched.name, limitOrder.status)
        assertEquals(1, limitOrder.loadMatchedOrders().size)

        assertEquals(1000.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "EUR" }?.volume)
        assertEquals(-1500.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "USD" }?.volume)
        assertEquals(-1000.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" }?.volume)
        assertEquals(1500.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" }?.volume)

        assertEquals(1000.0, testWalletDatabaseAcessor.getBalance("Client3", "EUR"))
        assertEquals(1500.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"))
        assertEquals(1000.0, testWalletDatabaseAcessor.getBalance("Client4", "EUR"))
        assertEquals(1500.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testMatchOneToMany() {
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", price = 1.5, volume = 100.0, clientId = "Client3", orderType = Buy.name))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(partitionKey = "EURUSD_Buy", price = 1.4, volume = 1000.0, clientId = "Client1", orderType = Buy.name))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 3000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 3000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 2000.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService)

        service.processMessage(buildByteArray(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = 1000.0, orderType = Sell.name)))

        val marketOrder = testDatabaseAccessor.getLastOrder()
        assertEquals(Matched.name, marketOrder.status)
        assertEquals(1.41, marketOrder.price)
        assertEquals(2, marketOrder.loadMatchedOrdersList().size)

        assertEquals(1, testLimitDatabaseAccessor.orders.size)
        assertEquals(1, testLimitDatabaseAccessor.ordersDone.size)

        val limitOrder = testLimitDatabaseAccessor.ordersDone.first()
        assertEquals(Matched.name, limitOrder.status)
        assertEquals(1, limitOrder.loadMatchedOrders().size)

        assertEquals(100.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "EUR" }?.volume)
        assertEquals(-150.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "USD" }?.volume)
        assertEquals(900.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client1" && it.assetId == "EUR" }?.volume)
        assertEquals(-1260.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client1" && it.assetId == "USD" }?.volume)
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" && it.volume == -100.0})
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" && it.volume == 150.0 })
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" && it.volume == -900.0})
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" && it.volume == 1260.0 })

        assertEquals(100.0, testWalletDatabaseAcessor.getBalance("Client3", "EUR"))
        assertEquals(2850.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"))
        assertEquals(900.0, testWalletDatabaseAcessor.getBalance("Client1", "EUR"))
        assertEquals(1740.0, testWalletDatabaseAcessor.getBalance("Client1", "USD"))
        assertEquals(1000.0, testWalletDatabaseAcessor.getBalance("Client4", "EUR"))
        assertEquals(1410.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"))
    }

    private fun buildByteArray(order: MarketOrder): ByteArray {
        return ProtocolMessages.MarketOrder.newBuilder()
                .setUid(order.rowKey.toLong())
                .setTimestamp(order.createdAt.time)
                .setClientId(order.getClientId())
                .setAssetId(order.assetPair)
                .setOrderAction(OrderSide.valueOf(order.orderType).side)
                .setBlockChain(order.blockChain)
                .setVolume(order.volume)
                .build().toByteArray()
    }
}

    fun buildMarketOrder(rowKey: String = "1",
                         assetId: String = "EURUSD",
                         clientId: String = "Client1",
                         matchedAt: Date? = null,
                         orderType: String = Buy.name,
                         registered: Date = Date(),
                         status: String = InOrderBook.name,
                         volume:Double = 1000.0): MarketOrder =
        MarketOrder(
                rowKey = rowKey,
                assetPair = assetId,
                clientId = clientId,
                matchedAt = matchedAt,
                orderType = orderType,
                blockChain = "",
                createdAt = registered,
                registered = Date(),
                status = status,
                volume = volume,
                matchedOrders = null
        )