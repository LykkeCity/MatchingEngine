package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class MinRemainingVolumeTest : AbstractTest() {

    @Before
    fun setUp() {
        testSettingsDatabaseAccessor.addTrustedClient("TrustedClient")
        testSettingsDatabaseAccessor.addTrustedClient("Client3")

        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 10000.0))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8, 0.01))
        initServices()
    }

    @Test
    fun testIncomingLimitOrder() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 8000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 8100.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 8200.0)))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1800.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1991, price = 9000.0)))

        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
        assertEquals(1, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)

        assertEquals(0.1, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(0.1, balancesHolder.getReservedBalance("Client1", "BTC"))

        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(3, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "order1" }.order.status)
    }

    @Test
    fun testIncomingLimitOrderWithMinRemaining() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.3))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1, price = 7000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1, price = 6900.0)))

        clearMessageQueues()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client2", assetId = "BTCUSD", volume = -0.2009, price = 6900.0)))

        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
        assertEquals(0, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)

        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))
        assertEquals(0.0, balancesHolder.getReservedBalance("Client2", "BTC"))
        assertEquals(0.1, testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(0.1, balancesHolder.getBalance("Client2", "BTC"))

        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(3, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "order1" }.order.status)
    }

    @Test
    fun testIncomingMarketOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.2))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1, price = 7000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.1009, price = 6900.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1, price = 6800.0)))

        clearMessageQueues()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2)))

        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(1, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)

        assertEquals(680.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(680.0, balancesHolder.getReservedBalance("Client1", "USD"))

        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "order1" }.order.status)
    }

    @Test
    fun testIncomingMultiLimitOrder() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 8000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 8100.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 8200.0)))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("TrustedClient", "USD", 1800.0))
        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "TrustedClient", listOf(
                VolumePrice(0.11, 9000.0), VolumePrice(0.0891, 8900.0)
        ), listOf(), listOf()))

        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
        assertEquals(1, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)

        assertEquals(0.1, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(0.1, balancesHolder.getReservedBalance("Client1", "BTC"))

        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "order1" }.order.status)
    }

    @Test
    fun testIncomingMultiLimitOrderWithMinRemaining() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("TrustedClient", "BTC", 0.3))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1, price = 7000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1, price = 6900.0)))

        clearMessageQueues()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "TrustedClient", listOf(
                VolumePrice(-0.11, 6800.0), VolumePrice(-0.0909, 6900.0)
        ), listOf(), listOf(), listOf("order1", "order2")))

        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCUSD", false).size)
        assertEquals(0, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)

        assertEquals(0.1, testWalletDatabaseAccessor.getBalance("TrustedClient", "BTC"))
        assertEquals(0.1, balancesHolder.getBalance("TrustedClient", "BTC"))

        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, report.orders.filter { it.order.externalId == "order2" }.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "order2" }.order.status)
    }
}