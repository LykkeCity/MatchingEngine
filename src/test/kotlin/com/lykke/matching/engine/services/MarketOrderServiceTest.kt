package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestLimitOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestMarketOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.order.OrderStatus.InOrderBook
import com.lykke.matching.engine.order.OrderStatus.Matched
import com.lykke.matching.engine.order.OrderStatus.NoLiquidity
import com.lykke.matching.engine.order.OrderStatus.NotEnoughFunds
import com.lykke.matching.engine.queue.transaction.Swap
import com.lykke.matching.engine.queue.transaction.Transaction
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertNotNull

class MarketOrderServiceTest {
    var testDatabaseAccessor = TestMarketOrderDatabaseAccessor()
    var testLimitDatabaseAccessor = TestLimitOrderDatabaseAccessor()
    var testWalletDatabaseAcessor = TestWalletDatabaseAccessor()
    val transactionQueue = LinkedBlockingQueue<Transaction>()
    val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()

    val DELTA = 1e-9

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testLimitDatabaseAccessor.clear()
        testWalletDatabaseAcessor.clear()
        transactionQueue.clear()
        tradesInfoQueue.clear()
    }

    @After
    fun tearDown() {
    }

    @Test
    fun testNoLiqudity() {
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client1"))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, transactionQueue)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, tradesInfoQueue)

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder()))
        assertEquals(NoLiquidity.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testNotEnoughFundsClientOrder() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.6, volume = 1000.0, clientId = "Client1"))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1500.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, transactionQueue)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, tradesInfoQueue)

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "EURUSD", volume = -1000.0)))
        assertEquals(NotEnoughFunds.name, testLimitDatabaseAccessor.orders.find { it.price == 1.6 }?.status)
    }

    @Test
    fun testNoLiqudityToFullyFill() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client2"))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 2000.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, transactionQueue)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, tradesInfoQueue)

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client2", assetId = "EURUSD", volume = -2000.0)))
        assertEquals(NoLiquidity.name, testDatabaseAccessor.getLastOrder().status)
        assertEquals(InOrderBook.name, testLimitDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testNotEnoughFundsMarketOrder() {
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1500.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 900.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, transactionQueue)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, tradesInfoQueue)

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))
        assertEquals(NotEnoughFunds.name, testDatabaseAccessor.getLastOrder().status)
    }

    @Test
    fun testMatchOneToOne() {
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 1000.0, clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 3000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 2000.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, transactionQueue)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, tradesInfoQueue)

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        val marketOrder = testDatabaseAccessor.orders.find { it.partitionKey == "OrderId" }!!
        assertEquals(Matched.name, marketOrder.status)
        assertEquals(1.5, marketOrder.price)
        assertEquals(1, testDatabaseAccessor.matchingData.filter { it.partitionKey == marketOrder.getId() }.size)
        assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        assertEquals(0, testLimitDatabaseAccessor.orders.size)
        assertEquals(2, testLimitDatabaseAccessor.ordersDone.size)

        val limitOrder = testLimitDatabaseAccessor.ordersDone.first()
        assertEquals(Matched.name, limitOrder.status)
        assertEquals(1, testDatabaseAccessor.matchingData.filter { it.partitionKey == limitOrder.getId() }.size)

        assertEquals(2, testLimitDatabaseAccessor.ordersDone.size)
        assertNotNull(testLimitDatabaseAccessor.ordersDone.find { it.partitionKey == "OrderId"})
        assertNotNull(testLimitDatabaseAccessor.ordersDone.find { it.partitionKey == "Client3"})

        assertEquals(1000.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "EUR" }?.volume)
        assertEquals(-1500.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "USD" }?.volume)
        assertEquals(-1000.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" }?.volume)
        assertEquals(1500.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" }?.volume)

        assertEquals(1000.0, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(1500.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(1000.0, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(1500.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        assertEquals("Client4", swap.clientId1)
        assertEquals(1000.0, swap.Amount1, DELTA)
        assertEquals("EUR", swap.origAsset1)
        assertEquals("Client3", swap.clientId2)
        assertEquals(1500.0, swap.Amount2, DELTA)
        assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testMatchOneToMany() {
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = 100.0, clientId = "Client3"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.4, volume = 1000.0, clientId = "Client1"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 3000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 3000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "EUR", 2000.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, transactionQueue)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, tradesInfoQueue)

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1000.0)))

        val marketOrder = testDatabaseAccessor.orders.find { it.partitionKey == "OrderId" }!!
        assertEquals(Matched.name, marketOrder.status)
        assertEquals(1.41, marketOrder.price)
        assertEquals(2, testDatabaseAccessor.matchingData.filter { it.partitionKey == marketOrder.getId() }.size)
        assertEquals(16, testDatabaseAccessor.orderTradesLinks.size)

        assertEquals(1, testLimitDatabaseAccessor.orders.size)
        assertEquals(2, testLimitDatabaseAccessor.ordersDone.size)

        val limitOrder = testLimitDatabaseAccessor.ordersDone.first()
        assertEquals(Matched.name, limitOrder.status)
        assertEquals(1, testDatabaseAccessor.matchingData.filter { it.partitionKey == limitOrder.getId() }.size)

        assertEquals(100.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "EUR" }?.volume)
        assertEquals(-150.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "USD" }?.volume)
        assertEquals(900.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client1" && it.assetId == "EUR" }?.volume)
        assertEquals(-1260.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client1" && it.assetId == "USD" }?.volume)
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" && it.volume == -100.0})
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" && it.volume == 150.0 })
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" && it.volume == -900.0})
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" && it.volume == 1260.0 })

        assertEquals(100.0, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(2850.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(900.0, testWalletDatabaseAcessor.getBalance("Client1", "EUR"), DELTA)
        assertEquals(1740.0, testWalletDatabaseAcessor.getBalance("Client1", "USD"), DELTA)
        assertEquals(1000.0, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(1410.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        var swap = transactionQueue.take() as Swap
        assertEquals("Client4", swap.clientId1)
        assertEquals(100.0, swap.Amount1, DELTA)
        assertEquals("EUR", swap.origAsset1)
        assertEquals("Client3", swap.clientId2)
        assertEquals(150.0, swap.Amount2, DELTA)
        assertEquals("USD", swap.origAsset2)

        swap = transactionQueue.take() as Swap
        assertEquals("Client4", swap.clientId1)
        assertEquals(900.0, swap.Amount1, DELTA)
        assertEquals("EUR", swap.origAsset1)
        assertEquals("Client1", swap.clientId2)
        assertEquals(1260.0, swap.Amount2, DELTA)
        assertEquals("USD", swap.origAsset2)
    }

    @Test
    fun testNotStraight() {
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = -500.0, assetId = "EURUSD", clientId = "Client3"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 1000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1000.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, transactionQueue)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, tradesInfoQueue)

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -750.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.find { it.partitionKey == "OrderId" }!!
        assertEquals(Matched.name, marketOrder.status)
        assertEquals(1.5, marketOrder.price)
        assertEquals(1, testDatabaseAccessor.matchingData.filter { it.partitionKey == marketOrder.getId() }.size)
        assertEquals(8, testDatabaseAccessor.orderTradesLinks.size)

        assertEquals(0, testLimitDatabaseAccessor.orders.size)
        assertEquals(2, testLimitDatabaseAccessor.ordersDone.size)

        val limitOrder = testLimitDatabaseAccessor.ordersDone.first()
        assertEquals(Matched.name, limitOrder.status)
        assertEquals(1, testDatabaseAccessor.matchingData.filter { it.partitionKey == limitOrder.getId() }.size)

        assertEquals(2, testLimitDatabaseAccessor.ordersDone.size)
        assertNotNull(testLimitDatabaseAccessor.ordersDone.find { it.partitionKey == "OrderId"})
        assertNotNull(testLimitDatabaseAccessor.ordersDone.find { it.partitionKey == "Client3"})

        assertEquals(-500.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "EUR" }?.volume)
        assertEquals(750.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "USD" }?.volume)
        assertEquals(500.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" }?.volume)
        assertEquals(-750.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" }?.volume)

        assertEquals(500.0, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(750.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(500.0, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(250.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        val swap = transactionQueue.take() as Swap
        assertEquals("Client4", swap.clientId1)
        assertEquals(750.0, swap.Amount1, DELTA)
        assertEquals("USD", swap.origAsset1)
        assertEquals("Client3", swap.clientId2)
        assertEquals(500.0, swap.Amount2, DELTA)
        assertEquals("EUR", swap.origAsset2)
    }

    @Test
    fun testNotStraightMatchOneToMany() {
        testWalletDatabaseAcessor.addAssetPair(AssetPair("EUR", "USD"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.4, volume = -100.0, clientId = "Client3"))
        testLimitDatabaseAccessor.addLimitOrder(buildLimitOrder(price = 1.5, volume = -1000.0, clientId = "Client1"))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 3000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client3", "EUR", 3000.0))
        testWalletDatabaseAcessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 2000.0))

        val cashOperationService = CashOperationService(testWalletDatabaseAcessor, transactionQueue)
        val limitOrderService = LimitOrderService(testLimitDatabaseAccessor, cashOperationService)
        val service = MarketOrderService(testDatabaseAccessor, limitOrderService, cashOperationService, transactionQueue, tradesInfoQueue)

        service.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client4", assetId = "EURUSD", volume = -1490.0, straight = false)))

        val marketOrder = testDatabaseAccessor.orders.find { it.partitionKey == "OrderId" }!!
        assertEquals(Matched.name, marketOrder.status)
        assertEquals(1.49, marketOrder.price!!, DELTA)
        assertEquals(2, testDatabaseAccessor.matchingData.filter { it.partitionKey == marketOrder.getId() }.size)
        assertEquals(16, testDatabaseAccessor.orderTradesLinks.size)

        assertEquals(1, testLimitDatabaseAccessor.orders.size)
        assertEquals(2, testLimitDatabaseAccessor.ordersDone.size)

        val limitOrder = testLimitDatabaseAccessor.ordersDone.first()
        assertEquals(Matched.name, limitOrder.status)
        assertEquals(1, testDatabaseAccessor.matchingData.filter { it.partitionKey == limitOrder.getId() }.size)

        assertEquals(-100.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "EUR" }!!.volume, DELTA)
        assertEquals(140.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client3" && it.assetId == "USD" }!!.volume, DELTA)
        assertEquals(-900.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client1" && it.assetId == "EUR" }!!.volume, DELTA)
        assertEquals(1350.0, testDatabaseAccessor.trades.find { it.getClientId() == "Client1" && it.assetId == "USD" }!!.volume, DELTA)
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" && it.volume == 100.0})
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" && it.volume == -140.0 })
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "EUR" && it.volume < 900.0})
        assertNotNull(testDatabaseAccessor.trades.find { it.getClientId() == "Client4" && it.assetId == "USD" && it.volume == -1350.0 })

        assertEquals(2900.0, testWalletDatabaseAcessor.getBalance("Client3", "EUR"), DELTA)
        assertEquals(140.0, testWalletDatabaseAcessor.getBalance("Client3", "USD"), DELTA)
        assertEquals(2100.0, testWalletDatabaseAcessor.getBalance("Client1", "EUR"), DELTA)
        assertEquals(1350.0, testWalletDatabaseAcessor.getBalance("Client1", "USD"), DELTA)
        assertEquals(1000.0, testWalletDatabaseAcessor.getBalance("Client4", "EUR"), DELTA)
        assertEquals(510.0, testWalletDatabaseAcessor.getBalance("Client4", "USD"), DELTA)

        var swap = transactionQueue.take() as Swap
        assertEquals("Client4", swap.clientId1)
        assertEquals(140.0, swap.Amount1, DELTA)
        assertEquals("USD", swap.origAsset1)
        assertEquals("Client3", swap.clientId2)
        assertEquals(100.0, swap.Amount2, DELTA)
        assertEquals("EUR", swap.origAsset2)

        swap = transactionQueue.take() as Swap
        assertEquals("Client4", swap.clientId1)
        assertEquals(1350.0, swap.Amount1, DELTA)
        assertEquals("USD", swap.origAsset1)
        assertEquals("Client1", swap.clientId2)
        assertEquals(900.0, swap.Amount2, DELTA)
        assertEquals("EUR", swap.origAsset2)
    }


    private fun buildMarketOrderWrapper(order: MarketOrder): MessageWrapper {
        return MessageWrapper(MessageType.MARKET_ORDER, ProtocolMessages.MarketOrder.newBuilder()
                .setUid(Date().time)
                .setTimestamp(order.createdAt.time)
                .setClientId(order.clientId)
                .setAssetPairId(order.assetPairId)
                .setVolume(order.volume)
                .setStraight(order.straight)
                .build().toByteArray(), null)
    }
}

    fun buildMarketOrder(rowKey: String = UUID.randomUUID().toString(),
                         assetId: String = "EURUSD",
                         clientId: String = "Client1",
                         registered: Date = Date(),
                         status: String = InOrderBook.name,
                         straight: Boolean = true,
                         volume: Double = 1000.0): MarketOrder =
        MarketOrder(
                uid = rowKey,
                assetPairId = assetId,
                clientId = clientId,
                createdAt = registered,
                registered = Date(),
                status = status,
                volume = volume,
                straight = straight
        )