package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

class LimitOrderCancelServiceTest : AbstractTest() {

    @Before
    fun setUp() {
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "5", price = 100.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "3", price = 300.0, volume = -1.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "6", price = 200.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "7", price = 300.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "8", price = 400.0))

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 1000.0, reservedBalance = 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
        initServices()
    }

    @Test
    fun testCancel() {
        limitOrderCancelService.processMessage(MessageBuilder.buildLimitOrderCancelWrapper("3"))

        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)

        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(OrderStatus.Cancelled.name, (clientsLimitOrdersQueue.poll() as LimitOrdersReport).orders.first().order.status)
        assertEquals(1, balanceNotificationQueue.size)
        assertEquals("Client1", balanceNotificationQueue.poll().clientId)
        assertEquals(1, balanceUpdateQueue.size)

        val balanceUpdate = balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(1, balanceUpdate.balances.size)
        assertEquals("Client1", balanceUpdate.balances.first().id)
        assertEquals("EUR", balanceUpdate.balances.first().asset)
        assertEquals(1000.0, balanceUpdate.balances.first().oldBalance)
        assertEquals(1000.0, balanceUpdate.balances.first().newBalance)
        assertEquals(1.0, balanceUpdate.balances.first().oldReserved)
        assertEquals(0.0, balanceUpdate.balances.first().newReserved)

        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "EUR"))

        val order = testOrderDatabaseAccessor.loadLimitOrders().find { it.id == "3" }
        assertNull(order)
        assertEquals(4, testOrderDatabaseAccessor.loadLimitOrders().size)

        val previousOrders = genericLimitOrderService.getAllPreviousOrders("Client1", "EURUSD", true)
        assertEquals(4, previousOrders.size)
        assertFalse(previousOrders.any { it.externalId == "3" })
    }

    @Test
    fun testMultiCancel() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 5))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 1.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "10", clientId = "Client2", assetId = "BTCUSD", price = 9000.0, volume = -0.5)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "11", clientId = "Client2", assetId = "BTCUSD", price = 9100.0, volume = -0.3)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "12", clientId = "Client2", assetId = "BTCUSD", price = 9200.0, volume = -0.2)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "13", clientId = "Client2", assetId = "BTCUSD", price = 8000.0, volume = 0.1)))
        clearMessageQueues()

        limitOrderCancelService.processMessage(MessageBuilder.buildLimitOrderCancelWrapper(listOf("10", "11", "13")))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 0)

        assertBalance("Client2", "BTC", 1.0, 0.2)
        assertBalance("Client2", "USD", 1000.0, 0.0)

        assertEquals(2, orderBookQueue.size)
        assertEquals(2, rabbitOrderBookQueue.size)
        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(3, report.orders.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "10" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "11" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "13" }.order.status)

        assertEquals(1, balanceUpdateQueue.size)
        val balanceUpdate = balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(MessageType.LIMIT_ORDER_CANCEL.name, balanceUpdate.type)
        assertEquals(2, balanceUpdate.balances.size)

        val btc = balanceUpdate.balances.first { it.asset == "BTC" }
        assertEquals("Client2", btc.id)
        assertEquals(1.0, btc.oldReserved)
        assertEquals(0.2, btc.newReserved)

        val usd = balanceUpdate.balances.first { it.asset == "USD" }
        assertEquals("Client2", usd.id)
        assertEquals(800.0, usd.oldReserved)
        assertEquals(0.0, usd.newReserved)
    }
}