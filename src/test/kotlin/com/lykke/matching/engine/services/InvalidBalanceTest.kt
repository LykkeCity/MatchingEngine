package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildTransferWrapper
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class InvalidBalanceTest : AbstractTest() {

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("ETH", 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("ETHUSD", "ETH", "USD", 5))
    }

    @Test
    fun testLimitOrderLeadsToInvalidBalance() {

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 0.02, reservedBalance = 0.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "ETH", balance = 1000.0, reservedBalance = 0.0))

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", volume = -0.000005, price = 1000.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", volume = -0.000005, price = 1000.0))

        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "ETHUSD", price = 1000.0, volume = 0.00002)))

        assertEquals(0, trustedClientsLimitOrdersQueue.size)
        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals("Client1", report.orders.first().order.clientId)
        assertEquals(OrderStatus.NotEnoughFunds.name, report.orders.first().order.status)

        assertEquals(0, orderBookQueue.size)
        assertEquals(0, rabbitOrderBookQueue.size)
        assertEquals(0, lkkTradesQueue.size)
        assertEquals(0, tradesInfoQueue.size)

        assertEquals(0, genericLimitOrderService.getOrderBook("ETHUSD").getOrderBook(true).size)
        assertEquals(0, testOrderDatabaseAccessor.getOrders("ETHUSD", true).size)
        assertEquals(2, genericLimitOrderService.getOrderBook("ETHUSD").getOrderBook(false).size)
        assertEquals(2, testOrderDatabaseAccessor.getOrders("ETHUSD", false).size)
        genericLimitOrderService.getOrderBook("ETHUSD").getOrderBook(false).forEach {
            assertEquals("Client2", it.clientId)
            assertEquals(-0.000005, it.remainingVolume)
            assertEquals(OrderStatus.InOrderBook.name, it.status)
        }

        assertBalance("Client1", "USD", 0.02, 0.0)
        assertEquals(0.0, balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(0.0, testWalletDatabaseAccessor.getBalance("Client1", "ETH"))

        assertBalance("Client2", "ETH", 1000.0, 0.0)
        assertEquals(0.0, balancesHolder.getReservedBalance("Client2", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))

        assertOriginWallets()
    }

    @Test
    fun testMarketOrderWithPreviousInvalidBalance() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 0.02, reservedBalance = 0.0))

        // invalid opposite wallet
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "ETH", balance = 1.0, reservedBalance = 1.1))

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", volume = -0.000005, price = 1000.0))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", volume = -0.000005, price = 1000.0))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "ETHUSD", volume = 0.00001)))

        assertEquals(0, trustedClientsLimitOrdersQueue.size)
        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(1, rabbitSwapQueue.size)

        val limitReport = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, limitReport.orders.size)
        limitReport.orders.forEach {
            assertEquals("Client2", it.order.clientId)
            assertEquals(OrderStatus.Matched.name, it.order.status)
        }

        val report = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals("Client1", report.order.clientId)
        assertEquals(OrderStatus.Matched.name, report.order.status)

        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)
        assertEquals(1, lkkTradesQueue.size)
        assertEquals(4, lkkTradesQueue.poll().size)
        assertEquals(0, tradesInfoQueue.size)

        assertEquals(0, genericLimitOrderService.getOrderBook("ETHUSD").getOrderBook(true).size)
        assertEquals(0, testOrderDatabaseAccessor.getOrders("ETHUSD", true).size)
        assertEquals(0, genericLimitOrderService.getOrderBook("ETHUSD").getOrderBook(false).size)
        assertEquals(0, testOrderDatabaseAccessor.getOrders("ETHUSD", false).size)

        assertBalance("Client1", "USD", 0.0, 0.0)
        assertEquals(0.00001, balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(0.00001, testWalletDatabaseAccessor.getBalance("Client1", "ETH"))

        assertBalance("Client2", "ETH", 0.99999, 1.09999)
        assertEquals(0.02, balancesHolder.getBalance("Client2", "USD"))
        assertEquals(0.02, testWalletDatabaseAccessor.getBalance("Client2", "USD"))

        assertOriginWallets()
    }

    @Test
    fun testNegativeBalanceDueToTransferWithOverdraftLimit() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 3.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "ETH", 3.0))

        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "ETHUSD", price = 1.0, volume = 3.0)))

        assertBalance("Client1", "USD", 3.0, 3.0)

        val message = buildTransferWrapper("Client1", "Client2", "USD", 4.0, 4.0)
        cashTransferOperationsService.parseMessage(message)
        cashTransferOperationsService.processMessage(message)

        assertBalance("Client1", "USD", -1.0, 3.0)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "ETHUSD", price = 1.1, volume = -0.5)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", price = 1.1, volume = 0.5)))

        assertBalance("Client1", "USD", -0.45, 3.0)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", price = 1.0, volume = -0.5)))

        assertBalance("Client1", "USD", -0.95, 2.5)

        assertOriginWallets()
    }

    private fun assertBalance(clientId: String, assetId: String, balance: Double, reservedBalance: Double) {
        assertEquals(balance, balancesHolder.getBalance(clientId, assetId))
        assertEquals(reservedBalance, balancesHolder.getReservedBalance(clientId, assetId))
        assertEquals(balance, testWalletDatabaseAccessor.getBalance(clientId, assetId))
        assertEquals(reservedBalance, testWalletDatabaseAccessor.getReservedBalance(clientId, assetId))
    }

    private fun assertOriginWallets() {
        balancesHolder.wallets.values.forEach { wallet ->
            wallet.balances.values.forEach {
                assertNull(it.originBalance)
                assertNull(it.originReserved)
            }
        }
    }
}