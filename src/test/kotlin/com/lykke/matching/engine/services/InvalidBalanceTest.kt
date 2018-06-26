package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildTransferWrapper
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import kotlin.test.assertEquals
import com.lykke.matching.engine.utils.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (InvalidBalanceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class InvalidBalanceTest : AbstractTest() {

    @Autowired
    private
    lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

    @TestConfiguration
    open class Config {

        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("ETH", 8))

            return testBackOfficeDatabaseAccessor
        }
    }
    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("ETHUSD", "ETH", "USD", 5))
    }

    @Test
    fun testLimitOrderLeadsToInvalidBalance() {

        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 0.02)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client1", assetId = "USD", reservedBalance = 0.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "ETH", balance = 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client2", assetId = "ETH", reservedBalance = 0.0)

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
            assertEquals(BigDecimal.valueOf(-0.000005), it.remainingVolume)
            assertEquals(OrderStatus.InOrderBook.name, it.status)
        }

        assertBalance("Client1", "USD", 0.02, 0.0)
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client1", "ETH"))

        assertBalance("Client2", "ETH", 1000.0, 0.0)
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client2", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))
    }

    @Test
    fun testMarketOrderWithPreviousInvalidBalance() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 0.02)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client1", assetId = "USD", reservedBalance = 0.0)

        // invalid opposite wallet
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "ETH", balance = 1.0)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client2", assetId = "ETH", reservedBalance = 1.1)

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
        assertEquals(BigDecimal.valueOf(0.00001), balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(0.00001), testWalletDatabaseAccessor.getBalance("Client1", "ETH"))

        assertBalance("Client2", "ETH", 0.99999, 1.09999)
        assertEquals(BigDecimal.valueOf(0.02), balancesHolder.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(0.02), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testNegativeBalanceDueToTransferWithOverdraftLimit() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 3.0)
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 3.0)

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

        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", price = 1.0, volume = -0.5)))

        assertBalance("Client1", "USD", -0.45, 0.0)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.clientId == "Client1" }.order.status)
    }

    @Test
    fun testMultiLimitOrderWithNotEnoughReservedFunds() {
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 0.25)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "ETH", reservedBalance = 0.09)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 275.0)

        initServices()

        testConfigDatabaseAccessor.addTrustedClient("Client1")
        applicationSettingsCache.update()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("ETHUSD", "Client1", listOf(
                VolumePrice(BigDecimal.valueOf(-0.1), BigDecimal.valueOf(1000.0)),
                VolumePrice(BigDecimal.valueOf(-0.05), BigDecimal.valueOf(1010.0)),
                VolumePrice(BigDecimal.valueOf(-0.1), BigDecimal.valueOf(1100.0))
        ), emptyList(), emptyList(), ordersUid = listOf("1", "2", "3")))
        testConfigDatabaseAccessor.clear()
        applicationSettingsCache.update()

        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "4", clientId = "Client2", assetId = "ETHUSD", volume = 0.25, price = 1100.0)))

        assertEquals(1, testOrderDatabaseAccessor.getOrders("ETHUSD", true).size)
        assertEquals(BigDecimal.valueOf(0.2), balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(0.04), balancesHolder.getReservedBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(0.05), balancesHolder.getBalance("Client2", "ETH"))
        assertEquals(BigDecimal.valueOf(224.5), balancesHolder.getBalance("Client2", "USD"))

        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(4, report.orders.size)

        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "1"}.order.status)
        assertEquals(OrderStatus.Matched.name, report.orders.first { it.order.externalId == "2"}.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "3"}.order.status)
        assertEquals(OrderStatus.Processing.name, report.orders.first { it.order.externalId == "4"}.order.status)
    }

    @Test
    fun `Test multi limit order with enough reserved but not enough main balance`() {
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 0.04)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "ETH", reservedBalance = 0.05)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 275.0)

        initServices()

        testConfigDatabaseAccessor.addTrustedClient("Client1")
        applicationSettingsCache.update()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("ETHUSD", "Client1",
                listOf(VolumePrice(BigDecimal.valueOf(-0.05), BigDecimal.valueOf(1010.0))), emptyList(), emptyList(), ordersUid = listOf("1")))
        testConfigDatabaseAccessor.clear()
        applicationSettingsCache.update()

        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "2", clientId = "Client2", assetId = "ETHUSD", volume = 0.25, price = 1100.0)))

        assertEquals(0, testOrderDatabaseAccessor.getOrders("ETHUSD", false).size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("ETHUSD", true).size)
        assertEquals(BigDecimal.valueOf(0.04), balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(275.0), balancesHolder.getReservedBalance("Client2", "USD"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "ETH"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client2", "ETH"))
        assertEquals(BigDecimal.valueOf(275.0), balancesHolder.getBalance("Client2", "USD"))

        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)

        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "1"}.order.status)
        assertEquals(OrderStatus.InOrderBook.name, report.orders.first { it.order.externalId == "2"}.order.status)
    }

    private fun assertBalance(clientId: String, assetId: String, balance: BigDecimal, reservedBalance: BigDecimal) {
        assertEquals(balance, balancesHolder.getBalance(clientId, assetId))
        assertEquals(reservedBalance, balancesHolder.getReservedBalance(clientId, assetId))
        assertEquals(balance, testWalletDatabaseAccessor.getBalance(clientId, assetId))
        assertEquals(reservedBalance, testWalletDatabaseAccessor.getReservedBalance(clientId, assetId))
    }
}