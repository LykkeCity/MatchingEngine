
package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderType
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderCancelWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.getSetting
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
import java.util.Date
import kotlin.test.assertNull
import kotlin.test.assertTrue
import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType as OutgoingMessageType
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (StopLimitOrderTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class StopLimitOrderTest : AbstractTest() {


    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    @TestConfiguration
    open class Config {

        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

            return testBackOfficeDatabaseAccessor
        }
    }


    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 0.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 0.0)

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 6))
        initServices()
    }

    @Test
    fun testNotEnoughFunds() {
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -1.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))

        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(OrderStatus.NotEnoughFunds.name, report.orders.first().order.status)

        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(executionEvent.header.eventType, MessageType.LIMIT_ORDER.name)
        assertEquals(executionEvent.header.messageType, OutgoingMessageType.ORDER)
        assertEquals(1, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, executionEvent.orders.first().status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, executionEvent.orders.first().rejectReason)
        assertEquals(0, executionEvent.balanceUpdates!!.size)
    }

    @Test
    fun testAddStopLimitOrder() {
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(OrderStatus.Pending.name, report.orders.first().order.status)
        assertEquals(BigDecimal.valueOf( 0.01), report.orders.first().order.reservedLimitVolume)
        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(BigDecimal.valueOf(0.01), balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(0.01), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(executionEvent.header.eventType, MessageType.LIMIT_ORDER.name)
        assertEquals(executionEvent.header.messageType, OutgoingMessageType.ORDER)
        assertEquals(1, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.PENDING, executionEvent.orders.first().status)
        assertNull(executionEvent.orders.first().rejectReason)
        assertEquals(1, executionEvent.balanceUpdates!!.size)
        assertEventBalanceUpdate("Client1", "BTC", "1", "1", "0", "0.01", executionEvent.balanceUpdates!!)
    }

    @Test
    fun testAddStopLimitOrderAndCancelAllPrevious() {
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))
        clearMessageQueues()
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = -0.02,
                lowerLimitPrice = 9500.0, lowerPrice = 9000.0, upperLimitPrice = 10500.0, upperPrice = 10000.0

        ), true))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.status == OrderStatus.Cancelled.name }.size)
        assertEquals(1, report.orders.filter { it.order.status == OrderStatus.Pending.name }.size)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(BigDecimal.valueOf(-0.02), genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).first().volume)
        assertEquals(BigDecimal.valueOf(0.02), balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(0.02), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(executionEvent.header.eventType, MessageType.LIMIT_ORDER.name)
        assertEquals(executionEvent.header.messageType, OutgoingMessageType.ORDER)
        assertEquals(2, executionEvent.orders.size)
        assertEquals(1, executionEvent.orders.filter { it.status == OutgoingOrderStatus.PENDING }.size)
        assertEquals(1, executionEvent.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }.size)
        assertEquals(1, executionEvent.balanceUpdates!!.size)
        assertEventBalanceUpdate("Client1", "BTC", "1", "1", "0.01", "0.02", executionEvent.balanceUpdates!!)
    }

    @Test
    fun testCancelStopLimitOrder() {
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))

        clearMessageQueues()
        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper("order1"))

        assertTrue(stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).isEmpty())
        assertTrue(genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first().order.status)

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(executionEvent.header.eventType, MessageType.LIMIT_ORDER_CANCEL.name)
        assertEquals(executionEvent.header.messageType, OutgoingMessageType.ORDER)
        assertEquals(1, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders.first().status)
        assertNull(executionEvent.orders.first().rejectReason)
        assertEquals(1, executionEvent.balanceUpdates!!.size)
        assertEventBalanceUpdate("Client1", "BTC", "1", "1", "0.01", "0", executionEvent.balanceUpdates!!)
    }

    @Test
    fun testProcessStopLimitOrder() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1.0)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                uid = "order2", clientId = "Client1", assetId = "BTCUSD", volume = -0.03,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.5, lowerPrice = 9000.0
        )))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.03, price = 10000.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.03, price = 9501.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.03, price = 9499.0)))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.03, price = 10000.0)))
        clearMessageQueues()
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.03, price = 9501.0)))

        assertEquals(BigDecimal.valueOf(0.01), balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(0.01), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(2, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().last() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order2" }.size)
        val stopOrder = report.orders.first { it.order.externalId == "order2" }

        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(BigDecimal.valueOf(9000.0), stopOrder.order.price)

        assertEquals(2, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(executionEvent.header.eventType, MessageType.LIMIT_ORDER.name)
        assertEquals(executionEvent.header.messageType, OutgoingMessageType.ORDER)
        assertEquals(2, executionEvent.orders.size)
        val eventStopOrder = executionEvent.orders.single { it.externalId == "order2" }
        assertEquals(OutgoingOrderStatus.MATCHED, eventStopOrder.status)
        assertEquals(4, executionEvent.balanceUpdates!!.size)
        assertEventBalanceUpdate("Client1", "BTC", "1", "0.97", "0.04", "0.01", executionEvent.balanceUpdates!!)
    }

    @Test
    fun testProcessStopOrderAfterRejectLimitOrderWithCancelPrevious() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.1)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 0.1)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 10000.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.1, price = 11000.0)))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = 0.05, type = LimitOrderType.STOP_LIMIT,
                upperLimitPrice = 10500.0, upperPrice = 11000.0
        )))

        clearMessageQueues()
        // cancel previous orders and will be rejected due to not enough funds
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2, price = 10000.0), true))

        assertEquals(2, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().last() as LimitOrdersReport

        assertEquals(2, report.orders.size)
        val clientOrders = report.orders.filter { it.order.clientId == "Client1" }
        assertEquals(1, clientOrders.size)
        val clientOrder = clientOrders.first()
        assertEquals(1, clientOrder.trades.size)
        assertEquals("550.00", clientOrder.trades.first().volume)
        assertEquals("0.05000000", clientOrder.trades.first().oppositeVolume)

        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)

        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(2, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(executionEvent.header.eventType, MessageType.LIMIT_ORDER.name)
        assertEquals(executionEvent.header.messageType, OutgoingMessageType.ORDER)
        assertEquals(2, executionEvent.orders.size)
        val eventClientOrder = executionEvent.orders.single { it.walletId == "Client1" }
        assertEquals(1, eventClientOrder.trades?.size)
        assertEquals("BTC", eventClientOrder.trades!!.first().baseAssetId)
        assertEquals("0.05", eventClientOrder.trades!!.first().baseVolume)
        assertEquals("USD", eventClientOrder.trades!!.first().quotingAssetId)
        assertEquals("-550", eventClientOrder.trades!!.first().quotingVolume)
    }

    @Test
    fun testProcessStopLimitOrderAfterLimitOrderCancellation() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.3)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 10000.0, upperPrice = 10500.0
        )))
        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.valueOf(945.0), balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(945.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", uid = "order2", assetId = "BTCUSD", volume = -0.1, price = 9000.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2, price = 10000.0)))

        clearMessageQueues()
        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper("order2"))

        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(2, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().last() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(BigDecimal.valueOf(10500.0), stopOrder.order.price)

        assertEquals(2, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(2, executionEvent.orders.size)
        val eventClientOrder = executionEvent.orders.single { it.externalId == "order1" }
        assertEquals(OutgoingOrderStatus.MATCHED, eventClientOrder.status)
        assertEquals("10500", eventClientOrder.price)
    }

    @Test
    fun testRejectStopOrderDuringMatching() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1.0)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.03, price = 9600.0)))

        // Order leading to negative spread. Added to reject stop order.
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.03, price = 9500.0)))

        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.5, lowerPrice = 9000.0
        )))

        clearMessageQueues()
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.03, price = 9600.0)))

        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
        assertEquals(1, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)

        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(2, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().last() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.status == OrderStatus.LeadToNegativeSpread.name }.size)

        assertEquals(2, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(1, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, executionEvent.orders.first().status)
        assertEquals(OrderRejectReason.LEAD_TO_NEGATIVE_SPREAD, executionEvent.orders.first().rejectReason)
    }

    @Test
    fun testProcessStopLimitOrderAfterMarketOrder() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.3)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1900.0)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 10000.0, upperPrice = 10500.0
        )))
        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.valueOf(945.0), balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(945.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 9000.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2, price = 10000.0)))

        clearMessageQueues()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "BTCUSD", volume = 0.2)))

        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(2, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().last() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(BigDecimal.valueOf( 10500.0), stopOrder.order.price)

        assertEquals(2, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(2, executionEvent.orders.size)
        assertEquals(1, executionEvent.orders.filter { it.externalId == "order1" }.size)

        val eventStopOrder = executionEvent.orders.first { it.externalId == "order1" }
        assertEquals(OutgoingOrderStatus.MATCHED, eventStopOrder.status)
        assertEquals("10500", eventStopOrder.price)
    }

    private fun processStopLimitOrderAfterMultiLimitOrder(forTrustedClient: Boolean) {
        if (forTrustedClient) {
            testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("Client3"))
        }

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.3)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1.0)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 8500.0, lowerPrice = 9000.0, upperLimitPrice = 10000.0, upperPrice = 10500.0
        )))
        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.valueOf( 945.0), balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(945.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 9000.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2, price = 10000.0)))

        clearMessageQueues()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD",
                "Client3",
                listOf(IncomingLimitOrder(-0.1, 11000.0),
                        IncomingLimitOrder(-0.05, 8500.0))))
    }

    @Test
    fun testProcessStopLimitOrderAfterTrustedClientMultiLimitOrder() {
        processStopLimitOrderAfterMultiLimitOrder(true)

        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(215.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(215.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().last() as LimitOrdersReport
        assertEquals(3, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(BigDecimal.valueOf(9000.0), stopOrder.order.price)

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(3, executionEvent.orders.size)
        assertEquals(1, executionEvent.orders.filter { it.externalId == "order1" }.size)

        val eventStopOrder = executionEvent.orders.first { it.externalId == "order1" }
        assertEquals(OutgoingOrderStatus.MATCHED, eventStopOrder.status)
        assertEquals("9000", eventStopOrder.price)
    }

    @Test
    fun testProcessStopLimitOrderAfterClientMultiLimitOrder() {
        processStopLimitOrderAfterMultiLimitOrder(false)

        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(215.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(215.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(2, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().last() as LimitOrdersReport
        assertEquals(3, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(BigDecimal.valueOf(9000.0), stopOrder.order.price)

        assertEquals(2, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(3, executionEvent.orders.size)
        assertEquals(1, executionEvent.orders.filter { it.externalId == "order1" }.size)

        val eventStopOrder = executionEvent.orders.first { it.externalId == "order1" }
        assertEquals(OutgoingOrderStatus.MATCHED, eventStopOrder.status)
        assertEquals("9000", eventStopOrder.price)
    }

    private fun processStopLimitOrderAfterMultiLimitOrderCancellation(forTrustedClient: Boolean) {
        if (forTrustedClient) {
            testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("Client2"))
        }

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.3)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 0.1)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 10000.0, upperPrice = 10500.0
        )))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD",
                "Client2",
                listOf(IncomingLimitOrder(-0.1, 9000.0),
                        IncomingLimitOrder(-0.2, 10000.0))))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.1, price = 10000.0)))

        clearMessageQueues()
        val message = buildMultiLimitOrderCancelWrapper("Client2", "BTCUSD", false)
        multiLimitOrderCancelService.parseMessage(message)
        multiLimitOrderCancelService.processMessage(message)
    }

    @Test
    fun `process stop limit order after trusted client multi limit orders cancellation`() {
        processStopLimitOrderAfterMultiLimitOrderCancellation(true)

        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(BigDecimal.valueOf(10500.0), stopOrder.order.price)

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, executionEvent.orders.size)
        assertEquals(1, executionEvent.orders.filter { it.externalId == "order1" }.size)

        val eventStopOrder = executionEvent.orders.first { it.externalId == "order1" }
        assertEquals(OutgoingOrderStatus.MATCHED, eventStopOrder.status)
        assertEquals("10500", eventStopOrder.price)
    }

    @Test
    fun `process stop limit order after client multi limit orders cancellation`() {
        processStopLimitOrderAfterMultiLimitOrderCancellation(false)

        assertEquals(0, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(100.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(2, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().last() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(BigDecimal.valueOf(10500.0), stopOrder.order.price)

        assertEquals(2, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(2, executionEvent.orders.size)
        assertEquals(1, executionEvent.orders.filter { it.externalId == "order1" }.size)

        val eventStopOrder = executionEvent.orders.first { it.externalId == "order1" }
        assertEquals(OutgoingOrderStatus.MATCHED, eventStopOrder.status)
        assertEquals("10500", eventStopOrder.price)
    }

    @Test
    fun testProcessStopLimitOrderAfterMinVolumeOrdersCancellation() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 5.0)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 10000.0)

        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 2))
        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client2", listOf(
                IncomingLimitOrder(-0.00009, 10000.0),
                IncomingLimitOrder(-0.09, 11000.0))))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD", "Client2", listOf(
                IncomingLimitOrder(1.0, 1.1),
                IncomingLimitOrder(6.0, 1.0))))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 11000.0, upperPrice = 11000.0
        )))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "EURUSD", volume = -5.0,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 1.0, lowerPrice = 0.99
        )))

        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(2, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(1, genericStopLimitOrderService.getOrderBook("EURUSD").getOrderBook(false).size)
        assertEquals(2, genericLimitOrderService.getOrderBook("EURUSD").getOrderBook(true).size)
        assertEquals(BigDecimal.valueOf(990.0), balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(5.0), balancesHolder.getReservedBalance("Client1", "EUR"))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 5, BigDecimal.valueOf(0.0001)))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 2, BigDecimal.valueOf(5.0)))

        initServices()

        clearMessageQueues()
        minVolumeOrderCanceller.cancel()

        assertEquals(3, testClientLimitOrderListener.getCount())
        assertTrue(genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericStopLimitOrderService.getOrderBook("EURUSD").getOrderBook(false).isEmpty())
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(15.0), balancesHolder.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client1", "EUR"))

        assertEquals(3, clientsEventsQueue.size)
    }

    @Test
    fun testProcessStopLimitOrdersChain() {
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 10000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 10000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 1.0)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1, price = 10000.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1, price = 9500.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1, price = 9000.0)))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.2,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 10500.0, upperPrice = 10000.0
        )))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9500.0
        )))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.2,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9000.0, lowerPrice = 9000.0
        )))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client3", assetId = "BTCUSD", volume = 0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9000.0, lowerPrice = 9000.0
        )))

        assertEquals(3, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(3, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(BigDecimal.valueOf(0.5), balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf( 900.0), balancesHolder.getReservedBalance("Client3", "USD"))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1, price = 10500.0)))


        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertTrue(genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client3", "USD"))
    }

    @Test
    fun testProcessBothSideStopLimitOrders() {
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("Client2"))

        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1050.0)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.1)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 900.0)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9000.0, lowerPrice = 8500.0
        )))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client3", assetId = "BTCUSD", volume = 0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 10000.0, lowerPrice = 10500.0
        )))

        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(BigDecimal.valueOf( 0.1), balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(1050.0), balancesHolder.getReservedBalance("Client3", "USD"))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD",
                "Client2",
                listOf(IncomingLimitOrder(-0.1, 10000.0),
                        IncomingLimitOrder(0.1, 9000.0))))

        assertTrue(genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client3", "USD"))
    }

    @Test
    fun testProcessStopLimitOrderImmediately() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.1)
        initServices()
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 9900.0)))
        clearMessageQueues()
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9900.0, lowerPrice = 10000.0)))

        assertTrue(genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericStopLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).isEmpty())

        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(10.0), balancesHolder.getBalance("Client1", "USD"))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(BigDecimal.valueOf( 10000.0), stopOrder.order.price)

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, executionEvent.orders.size)
        assertEquals(1, executionEvent.orders.filter { it.externalId == "order1" }.size)

        val eventStopOrder = executionEvent.orders.first { it.externalId == "order1" }
        assertEquals(OutgoingOrderStatus.MATCHED, eventStopOrder.status)
        assertEquals("10000", eventStopOrder.price)
    }

    @Test
    fun testExpiredStopLimitOrder() {
        val order = buildLimitOrder(clientId = "Client1", assetId = "BTCUSD",
                type = LimitOrderType.STOP_LIMIT,
                volume = -1.0,
                lowerLimitPrice = 1.0,
                lowerPrice = 1.0,
                timeInForce = OrderTimeInForce.GTD,
                expiryTime = Date())

        Thread.sleep(10)
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(order))

        assertStopOrderBookSize("BTCUSD", false, 0)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single().status)
    }

    @Test
    fun testProcessExpiredStopLimitOrder() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1.0)
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD",
                type = LimitOrderType.STOP_LIMIT,
                volume = -1.0,
                lowerLimitPrice = 1.0,
                lowerPrice = 1.0,
                timeInForce = OrderTimeInForce.GTD,
                expiryTime = Date(Date().time + 300))))

        clearMessageQueues()

        Thread.sleep(500)

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD",
                volume = 1.0, price = 1.0)))

        assertStopOrderBookSize("BTCUSD", false, 0)
        assertOrderBookSize("BTCUSD", false, 0)
        assertOrderBookSize("BTCUSD", true, 1)

        assertEquals(2, clientsEventsQueue.size)
        val stopOrderEvent = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(1, stopOrderEvent.orders.size)
        assertEquals(1, stopOrderEvent.balanceUpdates?.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, stopOrderEvent.orders.single().status)

        assertBalance("Client1", "BTC", 1.0, 0.0)
    }

    @Test
    fun testProcessImmediateOrCancelStopLimitOrderWithTrades() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 0.5)
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD",
                type = LimitOrderType.STOP_LIMIT,
                volume = -1.0,
                lowerLimitPrice = 1.0,
                lowerPrice = 1.0,
                timeInForce = OrderTimeInForce.IOC)))

        clearMessageQueues()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD",
                volume = 0.5, price = 1.0)))

        assertStopOrderBookSize("BTCUSD", false, 0)
        assertOrderBookSize("BTCUSD", false, 0)
        assertOrderBookSize("BTCUSD", true, 0)

        assertEquals(2, clientsEventsQueue.size)

        val event = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(4, event.balanceUpdates?.size)

        val eventStopOrder = event.orders.single { it.orderType == OrderType.STOP_LIMIT }
        assertEquals(OutgoingOrderStatus.CANCELLED, eventStopOrder.status)
        assertEquals(1, eventStopOrder.trades?.size)

        assertBalance("Client1", "BTC", 0.5, 0.0)
    }

    @Test
    fun testProcessImmediateOrCancelStopLimitOrderWithoutTrades() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 0.5)
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD",
                type = LimitOrderType.STOP_LIMIT,
                volume = -1.0,
                lowerLimitPrice = 0.99,
                lowerPrice = 0.98,
                timeInForce = OrderTimeInForce.IOC)))

        clearMessageQueues()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD",
                volume = 0.5, price = 0.97)))

        assertStopOrderBookSize("BTCUSD", false, 0)
        assertOrderBookSize("BTCUSD", false, 0)
        assertOrderBookSize("BTCUSD", true, 1)

        assertEquals(2, clientsEventsQueue.size)

        val event = clientsEventsQueue.last() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(1, event.balanceUpdates?.size)

        val eventStopOrder = event.orders.single { it.orderType == OrderType.STOP_LIMIT }
        assertEquals(OrderType.STOP_LIMIT, eventStopOrder.orderType)
        assertEquals(OutgoingOrderStatus.CANCELLED, eventStopOrder.status)
        assertEquals(0, eventStopOrder.trades?.size)

        assertBalance("Client1", "BTC", 1.0, 0.0)
    }

}