package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class LimitOrderServiceDustTest : AbstractTest() {

    @Before
    fun setUp() {
        testSettingsDatabaseAccessor.addTrustedClient("Client3")
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
        initServices()
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust1() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = -0.05)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        var result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = 0.04997355)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(1000 - 0.04997355, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(1159.92, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(1000 + 0.04997355, testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(840.08, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust2() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = -0.05)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        var result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = 0.05002635)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)

        assertEquals(999.95, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(1160.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(1000.05, testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(840.0, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust3() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = 0.05)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        var result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = -0.04997355)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(1000 + 0.04997355, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(840.09, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(1000 - 0.04997355, testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(1159.91, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust4() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = 0.05)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        var result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = -0.05002635)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)

        assertEquals(1000.05, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(840.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(999.95, testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(1160.0, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust5() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = -0.05)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        var result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = 0.0499727)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(999.9500273, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(1159.92, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(1000.0499727, testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(840.08, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust6() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client1", price = 3200.0, volume = 0.05)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        var result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", clientId = "Client2", price = 3200.0, volume = -0.0499727)))
        assertEquals(1, clientsLimitOrdersQueue.size)
        result = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)

        assertEquals(1000.0499727, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(840.09, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(999.9500273, testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(1159.91, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
    }

}