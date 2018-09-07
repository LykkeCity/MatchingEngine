package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
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

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

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
        initServices()
    }

    @Test
    fun testLimitOrderLeadsToInvalidBalance() {

        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 0.02)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client1", assetId = "USD", reservedBalance = 0.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "ETH", balance = 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client2", assetId = "ETH", reservedBalance = 0.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", volume = -0.000005, price = 1000.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", volume = -0.000005, price = 1000.0))

        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "ETHUSD", price = 1000.0, volume = 0.00002)))

        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())
        assertEquals(1, testClientLimitOrderListener.getCount())

        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals("Client1", report.orders.first().order.clientId)
        assertEquals(OrderStatus.NotEnoughFunds.name, report.orders.first().order.status)

        assertEquals(0, trustedClientsEventsQueue.size)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single().status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, event.orders.single().rejectReason)

        assertEquals(0, testOrderBookListener.getCount())
        assertEquals(0, testRabbitOrderBookListener.getCount())
        assertEquals(0, testLkkTradeListener.getCount())
        assertEquals(0, tradesInfoListener.getCount())

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

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", volume = -0.000005, price = 1000.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", volume = -0.000005, price = 1000.0))

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client1", assetId = "ETHUSD", volume = 0.00001)))

        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())
        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(1, rabbitSwapListener.getCount())

        val limitReport = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, limitReport.orders.size)
        limitReport.orders.forEach {
            assertEquals("Client2", it.order.clientId)
            assertEquals(OrderStatus.Matched.name, it.order.status)
        }

        val report = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals("Client1", report.order.clientId)
        assertEquals(OrderStatus.Matched.name, report.order.status)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        event.orders.forEach {
            assertEquals(OutgoingOrderStatus.MATCHED, it.status)
        }

        assertEquals(1, testOrderBookListener.getCount())
        assertEquals(1, testRabbitOrderBookListener.getCount())
        assertEquals(1, testLkkTradeListener.getCount())
        assertEquals(4, testLkkTradeListener.getQueue().poll().size)
        assertEquals(1, tradesInfoListener.getCount())

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

        cashTransferOperationsService.processMessage(messageBuilder.buildTransferWrapper("Client1", "Client2", "USD", 4.0, 4.0))

        assertBalance("Client1", "USD", -1.0, 3.0)

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "ETHUSD", price = 1.1, volume = -0.5)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", price = 1.1, volume = 0.5)))

        assertBalance("Client1", "USD", -0.45, 3.0)

        clearMessageQueues()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "ETHUSD", price = 1.0, volume = -0.5)))

        assertBalance("Client1", "USD", -0.45, 0.0)
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.clientId == "Client1" }.order.status)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.first { it.walletId == "Client1" }.status)

    }

    @Test
    fun testMultiLimitOrderWithNotEnoughReservedFunds() {
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 0.25)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 275.0)

        initServices()

        testConfigDatabaseAccessor.addTrustedClient("Client1")
        applicationSettingsCache.update()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("ETHUSD", "Client1", listOf(
                IncomingLimitOrder(-0.1, 1000.0, "1"),
                IncomingLimitOrder(-0.05, 1010.0, "2"),
                IncomingLimitOrder(-0.1, 1100.0, "3")
        )))
        testBalanceHolderWrapper.updateReservedBalance("Client1", "ETH", reservedBalance = 0.09)
        testConfigDatabaseAccessor.clear()
        applicationSettingsCache.update()

        clearMessageQueues()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "4", clientId = "Client2", assetId = "ETHUSD", volume = 0.25, price = 1100.0)))

        assertEquals(1, testOrderDatabaseAccessor.getOrders("ETHUSD", true).size)
        assertEquals(BigDecimal.valueOf(0.2), balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(0.04), balancesHolder.getReservedBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(0.05), balancesHolder.getBalance("Client2", "ETH"))
        assertEquals(BigDecimal.valueOf(224.5), balancesHolder.getBalance("Client2", "USD"))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(4, report.orders.size)

        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "1" }.order.status)
        assertEquals(OrderStatus.Matched.name, report.orders.first { it.order.externalId == "2" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "3" }.order.status)
        assertEquals(OrderStatus.Processing.name, report.orders.first { it.order.externalId == "4" }.order.status)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(4, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "1" }.status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.externalId == "2" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "3" }.status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders.single { it.externalId == "4" }.status)
    }

    @Test
    fun `Test multi limit order with enough reserved but not enough main balance`() {
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 0.1)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "ETH", reservedBalance = 0.05)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 275.0)

        initServices()

        testConfigDatabaseAccessor.addTrustedClient("Client1")
        applicationSettingsCache.update()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("ETHUSD", "Client1",
                listOf(IncomingLimitOrder(-0.05, 1010.0, "1"))))
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 0.04)
        testConfigDatabaseAccessor.clear()
        applicationSettingsCache.update()

        testClientLimitOrderListener.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "2", clientId = "Client2", assetId = "ETHUSD", volume = 0.25, price = 1100.0)))

        assertEquals(0, testOrderDatabaseAccessor.getOrders("ETHUSD", false).size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("ETHUSD", true).size)
        assertEquals(BigDecimal.valueOf(0.04), balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(275.0), balancesHolder.getReservedBalance("Client2", "USD"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "ETH"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client2", "ETH"))
        assertEquals(BigDecimal.valueOf(275.0), balancesHolder.getBalance("Client2", "USD"))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)

        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "1" }.order.status)
        assertEquals(OrderStatus.InOrderBook.name, report.orders.first { it.order.externalId == "2" }.order.status)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "1" }.status)
        assertEquals(OutgoingOrderStatus.PLACED, event.orders.single { it.externalId == "2" }.status)
    }
}