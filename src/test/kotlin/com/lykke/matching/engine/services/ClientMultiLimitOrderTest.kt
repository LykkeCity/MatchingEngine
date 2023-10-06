package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstructions
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
import kotlin.test.assertTrue
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (ClientMultiLimitOrderTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ClientMultiLimitOrderTest : AbstractTest() {


    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Autowired
    private lateinit var testSettingDatabaseAccessor: TestSettingsDatabaseAccessor

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)

        initServices()
    }

    @Test
    fun testAdd() {
        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        IncomingLimitOrder(-0.1, 10000.0, "1"),
                        IncomingLimitOrder(-0.2, 10500.0, "2"),
                        IncomingLimitOrder(-0.30000001, 11000.0, "3"),
                        IncomingLimitOrder(0.1, 9500.0, "4"),
                        IncomingLimitOrder(0.2, 9000.0, "5")
                )))

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 2)

        assertEquals(2, testOrderBookListener.getCount())
        assertEquals(2, testRabbitOrderBookListener.getCount())

        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertEquals(2, tradesInfoListener.getCount())

        assertBalance("Client1", "BTC", 1.0, 0.60000001)
        assertBalance("Client1", "USD", 3000.0, 2750.0)

        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(5, report.orders.size)
        report.orders.forEach {
            assertEquals(OrderStatus.InOrderBook.name, it.order.status)
        }
        assertEquals(BigDecimal.valueOf(0.1), report.orders.first { it.order.externalId == "1" }.order.reservedLimitVolume)
        assertEquals(BigDecimal.valueOf(0.2), report.orders.first { it.order.externalId == "2" }.order.reservedLimitVolume)
        assertEquals(BigDecimal.valueOf(0.30000001), report.orders.first { it.order.externalId == "3" }.order.reservedLimitVolume)
        assertEquals(BigDecimal.valueOf(950.0), report.orders.first { it.order.externalId == "4" }.order.reservedLimitVolume)
        assertEquals(BigDecimal.valueOf(1800.0), report.orders.first { it.order.externalId == "5" }.order.reservedLimitVolume)

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(0, trustedClientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "BTC", "1", "1", "0", "0.60000001", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client1", "USD", "3000", "3000", "0", "2750", event.balanceUpdates!!)
        assertEquals(5, event.orders.size)
        event.orders.forEach {
            assertEquals(OutgoingOrderStatus.PLACED, it.status)
            assertEquals(0, it.trades?.size)
        }
        assertEquals("-0.1", event.orders.single { it.externalId == "1" }.remainingVolume)
    }

    @Test
    fun testAddOneSide() {
        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        IncomingLimitOrder(-0.1, 10000.0),
                        IncomingLimitOrder(-0.2, 10500.0)
                )))

        assertOrderBookSize("BTCUSD", false, 2)
        assertOrderBookSize("BTCUSD", true, 0)

        assertEquals(1, testOrderBookListener.getCount())
        assertEquals(1, testRabbitOrderBookListener.getCount())
        assertEquals(1, tradesInfoListener.getCount())

        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertBalance("Client1", "BTC", 1.0, 0.3)
        assertBalance("Client1", "USD", 3000.0, 0.0)

        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(0, trustedClientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(1, event.balanceUpdates?.size)
    }

    @Test
    fun testCancelAllPrevious() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.2)
        initServices()
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 10500.0, volume = -0.2)))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-1", clientId = "Client1", assetId = "BTCUSD", price = 10100.0, volume = -0.4)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-2", clientId = "Client1", assetId = "BTCUSD", price = 11000.0, volume = -0.3)))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-3", clientId = "Client1", assetId = "BTCUSD", price = 9000.0, volume = 0.1)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-4", clientId = "Client1", assetId = "BTCUSD", price = 8000.0, volume = 0.2)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-5", clientId = "Client1", assetId = "BTCUSD", price = 7000.0, volume = 0.001)))

        clearMessageQueues()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        IncomingLimitOrder(-0.1, 10000.0),
                        IncomingLimitOrder(-0.2, 10500.0),
                        IncomingLimitOrder(-0.30000001, 11000.0),
                        IncomingLimitOrder(0.1, 9500.0),
                        IncomingLimitOrder(0.2, 9000.0)
                )))

        assertOrderBookSize("BTCUSD", false, 4)
        assertOrderBookSize("BTCUSD", true, 2)

        assertEquals(2, testOrderBookListener.getCount())
        assertEquals(2, testRabbitOrderBookListener.getCount())
        assertEquals(2, tradesInfoListener.getCount())

        val buyOrderBook = testOrderBookListener.getQueue().first { it.isBuy }
        val sellOrderBook = testOrderBookListener.getQueue().first { !it.isBuy }
        assertEquals(2, buyOrderBook.prices.size)
        assertEquals(BigDecimal.valueOf(9500.0), buyOrderBook.prices.first().price)
        assertEquals(4, sellOrderBook.prices.size)
        assertEquals(BigDecimal.valueOf(10000.0), sellOrderBook.prices.first().price)

        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertBalance("Client1", "BTC", 1.0, 0.60000001)
        assertBalance("Client1", "USD", 3000.0, 2750.0)
        assertBalance("Client2", "BTC", 0.2, 0.2)

        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(10, report.orders.size)

        val cancelledIds = report.orders.filter { it.order.status == OrderStatus.Cancelled.name }.map { it.order.externalId }.toMutableList()
        cancelledIds.sort()
        assertEquals(listOf("ForCancel-1", "ForCancel-2", "ForCancel-3", "ForCancel-4", "ForCancel-5"), cancelledIds)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.balanceUpdates?.size)
        assertEquals(10, event.orders.size)
        val eventCancelledIds = event.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }.map { it.externalId }.toMutableList()
        eventCancelledIds.sort()
        assertEquals(listOf("ForCancel-1", "ForCancel-2", "ForCancel-3", "ForCancel-4", "ForCancel-5"), eventCancelledIds)
    }

    @Test
    fun testCancelAllPreviousOneSide() {

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-1", clientId = "Client1", assetId = "BTCUSD", price = 10100.0, volume = -0.4)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "ForCancel-2", clientId = "Client1", assetId = "BTCUSD", price = 11000.0, volume = -0.3)))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 9000.0, volume = 0.1)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 8000.0, volume = 0.2)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 7000.0, volume = 0.001)))

        clearMessageQueues()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        IncomingLimitOrder(-0.1, 10000.0),
                        IncomingLimitOrder(-0.2, 10500.0),
                        IncomingLimitOrder(-0.30000001, 11000.0)
                )))

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 3)

        assertEquals(1, testOrderBookListener.getCount())
        assertEquals(1, testRabbitOrderBookListener.getCount())
        assertEquals(1, tradesInfoListener.getCount())

        val sellOrderBook = testOrderBookListener.getQueue().first { !it.isBuy }
        assertEquals(3, sellOrderBook.prices.size)
        assertEquals(BigDecimal.valueOf(10000.0), sellOrderBook.prices.first().price)

        assertBalance("Client1", "BTC", 1.0, 0.60000001)

        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(5, report.orders.size)

        val cancelledIds = report.orders.filter { it.order.status == OrderStatus.Cancelled.name }.map { it.order.externalId }.toMutableList()
        cancelledIds.sort()
        assertEquals(listOf("ForCancel-1", "ForCancel-2"), cancelledIds)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(5, event.orders.size)
        val eventCancelledIds = event.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }.map { it.externalId }.toMutableList()
        eventCancelledIds.sort()
        assertEquals(listOf("ForCancel-1", "ForCancel-2"), eventCancelledIds)
    }

    @Test
    fun testAddNotEnoughFundsOrder() {
        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(
                        IncomingLimitOrder(-0.1, 10000.0, "1"),
                        IncomingLimitOrder(-0.2, 10500.0, "2"),
                        IncomingLimitOrder(-0.30000001, 11000.0, "3"),
                        IncomingLimitOrder(-0.4, 12000.0, "ToReject-1"),
                        IncomingLimitOrder(0.1, 9500.0, "5"),
                        IncomingLimitOrder(0.2, 9000.0, "6"),
                        IncomingLimitOrder(0.03, 9500.0, "ToReject-2")
                )))

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 2)

        assertEquals(2, testOrderBookListener.getCount())
        assertEquals(2, testRabbitOrderBookListener.getCount())
        assertEquals(2, tradesInfoListener.getCount())

        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertBalance("Client1", "BTC", 1.0, 0.60000001)
        assertBalance("Client1", "USD", 3000.0, 2750.0)

        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(7, report.orders.size)
        assertEquals(OrderStatus.NotEnoughFunds.name, report.orders.first { it.order.externalId == "ToReject-1" }.order.status)
        assertEquals(OrderStatus.NotEnoughFunds.name, report.orders.first { it.order.externalId == "ToReject-2" }.order.status)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(MessageType.ORDER, event.header.messageType)
        assertEquals(7, event.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single { it.externalId == "ToReject-1" }.status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, event.orders.single { it.externalId == "ToReject-1" }.rejectReason)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single { it.externalId == "ToReject-2" }.status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, event.orders.single { it.externalId == "ToReject-2" }.rejectReason)
    }

    @Test
    fun testMatch() {
        testSettingDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("TrustedClient"))

        testBalanceHolderWrapper.updateBalance("Client1", "USD", 10000.0)

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 10000.0)

        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 0.2)

        testBalanceHolderWrapper.updateBalance("TrustedClient", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "USD", 3000.0)

        initServices()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "TrustedClient", listOf(
                IncomingLimitOrder(-0.3, 10800.0, "3"),
                IncomingLimitOrder(-0.4, 10900.0, "2"),
                IncomingLimitOrder(0.1, 9500.0, "6"),
                IncomingLimitOrder(0.2, 9300.0, "7")
        )))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(
                uid = "ToCancelDueToNoFundsForFee", clientId = "Client3", assetId = "BTCUSD", volume = -0.2, price = 10500.0,
                fees = buildLimitOrderFeeInstructions(FeeType.CLIENT_FEE, makerSize = 0.05, targetClientId = "TargetClient", assetIds = listOf("BTC"))
        )))

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client2", listOf(
                IncomingLimitOrder(-0.1, 10000.0, "5"),
                IncomingLimitOrder(-0.5, 11000.0, "1"),
                IncomingLimitOrder(0.3, 9000.0, "8"),
                IncomingLimitOrder(0.4, 8800.0, "9")
        )))

        clearMessageQueues()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                IncomingLimitOrder(-0.1, 11500.0, "14"),
                IncomingLimitOrder(0.05, 11000.0, "12"),
                IncomingLimitOrder(0.2, 10800.0, "13"),
                IncomingLimitOrder(0.1, 9900.0, "11")
        )))


        assertOrderBookSize("BTCUSD", false, 4)
        assertOrderBookSize("BTCUSD", true, 5)

        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())
        assertEquals(1, testClientLimitOrderListener.getCount())

        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(7, report.orders.size)

        val orderIds = report.orders.map { it.order.externalId }.toMutableList()
        orderIds.sort()
        assertEquals(listOf("11", "12", "13", "14", "3", "5", "ToCancelDueToNoFundsForFee"), orderIds)

        val matchedIds = report.orders.filter { it.order.status == OrderStatus.Matched.name }.map { it.order.externalId }.toMutableList()
        matchedIds.sort()
        assertEquals(listOf("12", "13", "5"), matchedIds)

        val cancelledIds = report.orders.filter { it.order.status == OrderStatus.Cancelled.name }.map { it.order.externalId }.toMutableList()
        cancelledIds.sort()
        assertEquals(listOf("ToCancelDueToNoFundsForFee"), cancelledIds)

        val addedIds = report.orders.filter { it.order.status == OrderStatus.InOrderBook.name }.map { it.order.externalId }.toMutableList()
        addedIds.sort()
        assertEquals(listOf("11", "14"), addedIds)

        val partiallyMatchedIds = report.orders.filter { it.order.status == OrderStatus.Processing.name }.map { it.order.externalId }.toMutableList()
        partiallyMatchedIds.sort()
        assertEquals(listOf("3"), partiallyMatchedIds)


        assertBalance("Client1", "BTC", 1.25, 0.1)
        assertBalance("Client1", "USD", 7380.0, 990.0)
        assertBalance("Client3", "BTC", 0.2, 0.0)

        assertBalance("Client2", "BTC", 0.9, 0.5)
        assertBalance("Client2", "USD", 11000.0, 6220.0)

        assertBalance("TrustedClient", "BTC", 0.85, 0.0)
        assertBalance("TrustedClient", "USD", 4620.0, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(0, trustedClientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(7, event.orders.size)

        val eventOrderIds = event.orders.map { it.externalId }.toMutableList()
        eventOrderIds.sort()
        assertEquals(listOf("11", "12", "13", "14", "3", "5", "ToCancelDueToNoFundsForFee"), eventOrderIds)

        val eventMatchedIds = event.orders.filter { it.status == OutgoingOrderStatus.MATCHED }.map { it.externalId }.toMutableList()
        eventMatchedIds.sort()
        assertEquals(listOf("12", "13", "5"), eventMatchedIds)

        val eventCancelledIds = event.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }.map { it.externalId }.toMutableList()
        eventCancelledIds.sort()
        assertEquals(listOf("ToCancelDueToNoFundsForFee"), eventCancelledIds)

        val eventAddedIds = event.orders.filter { it.status == OutgoingOrderStatus.PLACED }.map { it.externalId }.toMutableList()
        eventAddedIds.sort()
        assertEquals(listOf("11", "14"), eventAddedIds)

        val eventPartiallyMatchedIds = event.orders.filter { it.status == OutgoingOrderStatus.PARTIALLY_MATCHED }.map { it.externalId }.toMutableList()
        eventPartiallyMatchedIds.sort()
        assertEquals(listOf("3"), eventPartiallyMatchedIds)
    }

    @Test
    fun testNegativeSpread() {
        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                IncomingLimitOrder(-0.1, 10000.0),
                IncomingLimitOrder(0.1, 10100.0)
        )))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 0)

        assertBalance("Client1", "BTC", 1.0, 0.1)
        assertBalance("Client1", "USD", 3000.0, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "BTC", "1", "1", "0", "0.1", event.balanceUpdates!!)
    }

    @Test
    fun testCancelPreviousAndMatch() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.3)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2400.0)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 0.0)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.3, price = 9500.0)))

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                IncomingLimitOrder(0.1, 9000.0),
                IncomingLimitOrder(0.1, 8000.0),
                IncomingLimitOrder(0.1, 7000.0)
        )))

        clearMessageQueues()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                IncomingLimitOrder(0.1, 10000.0),
                IncomingLimitOrder(0.01, 9500.0),
                IncomingLimitOrder(0.1, 9000.0),
                IncomingLimitOrder(0.1, 8000.0)
        )))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 1)

        assertEquals(BigDecimal.valueOf(9000.0), genericLimitOrderService.getOrderBook("BTCUSD").getBidPrice())
        assertEquals(BigDecimal.valueOf(9500.0), genericLimitOrderService.getOrderBook("BTCUSD").getAskPrice())

        assertBalance("Client1", "USD", 1355.0, 900.0)
        assertBalance("Client1", "BTC", 0.11, 0.0)

        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(8, (testClientLimitOrderListener.getQueue().first() as LimitOrdersReport).orders.size)

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(8, (clientsEventsQueue.first() as ExecutionEvent).orders.size)
    }

    private fun setOrder() {
        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                IncomingLimitOrder(-0.4, 9200.0),
                IncomingLimitOrder(-0.3, 9100.0),
                IncomingLimitOrder(-0.2, 9000.0),
                IncomingLimitOrder(0.2, 7900.0),
                IncomingLimitOrder(0.1, 7800.0)
        )))
        clearMessageQueues()
    }

    @Test
    fun testEmptyOrderWithCancelPreviousBothSides() {
        setOrder()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1", orders = emptyList(),
                cancel = true, cancelMode = OrderCancelMode.BOTH_SIDES))

        assertOrderBookSize("BTCUSD", true, 0)
        assertOrderBookSize("BTCUSD", false, 0)
        val report = testClientLimitOrderListener.getQueue().first() as LimitOrdersReport
        assertEquals(5, report.orders.size)
        report.orders.forEach {
            assertEquals(OrderStatus.Cancelled.name, it.order.status)
        }

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(5, event.orders.size)
        event.orders.forEach {
            assertEquals(OutgoingOrderStatus.CANCELLED, it.status)
        }
    }

    @Test
    fun testOneSideOrderWithCancelPreviousBothSides() {
        setOrder()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(IncomingLimitOrder(-0.4, 9100.0, "1"),
                        IncomingLimitOrder(-0.3, 9000.0, "2")),
                cancel = true, cancelMode = OrderCancelMode.BOTH_SIDES))

        assertOrderBookSize("BTCUSD", true, 0)
        assertOrderBookSize("BTCUSD", false, 2)
        val report = testClientLimitOrderListener.getQueue().first() as LimitOrdersReport
        assertEquals(7, report.orders.size)

        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).map { it.externalId }.containsAll(listOf("1", "2")))

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(7, (clientsEventsQueue.poll() as ExecutionEvent).orders.size)
    }

    @Test
    fun testBothSidesOrderWithCancelPreviousOneSide() {
        setOrder()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1",
                listOf(IncomingLimitOrder(-0.01, 9100.0, "1"),
                        IncomingLimitOrder(-0.009, 9000.0, "2"),
                        IncomingLimitOrder(0.2, 7900.0, "3")),
                cancel = true, cancelMode = OrderCancelMode.BUY_SIDE))

        assertOrderBookSize("BTCUSD", true, 1)
        assertOrderBookSize("BTCUSD", false, 5)
        val report = testClientLimitOrderListener.getQueue().first() as LimitOrdersReport
        assertEquals(5, report.orders.size)

        assertEquals(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).map { it.externalId }, listOf("3"))

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(5, (clientsEventsQueue.poll() as ExecutionEvent).orders.size)
    }

    @Test
    fun testReplaceOrders() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.1)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC", 0.1)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "ClientOrder", clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 8000.0))
        initServices()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                IncomingLimitOrder(-0.4, 9300.0, "Ask-ToReplace-2"),
                IncomingLimitOrder(-0.3, 9200.0, "Ask-ToReplace-1"),
                IncomingLimitOrder(-0.2, 9100.0, "Ask-ToCancel-2"),
                IncomingLimitOrder(-0.1, 9000.0, "Ask-ToCancel-1"),
                IncomingLimitOrder(0.2, 7900.0, "Bid-ToReplace-1"),
                IncomingLimitOrder(0.1, 7800.0, "Bid-ToCancel-1"),
                IncomingLimitOrder(0.05, 7700.0, "Bid-ToReplace-2")
        )))
        clearMessageQueues()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                IncomingLimitOrder(-0.2, 9400.0, "NotFoundPrevious-1", oldUid = "NotExist-1"),
                IncomingLimitOrder(-0.2, 9300.0, "ask2", oldUid = "Ask-ToReplace-2"),
                IncomingLimitOrder(-0.3, 9200.0, "ask3", oldUid = "Ask-ToReplace-1"),
                IncomingLimitOrder(-0.2, 9100.0, "ask4"),
                IncomingLimitOrder(-0.3001, 9000.0, "NotEnoughFunds"),
                IncomingLimitOrder(0.11, 8000.0, "bid1", oldUid = "Bid-ToReplace-1"),
                IncomingLimitOrder(0.1, 7900.0, "bid2", oldUid = "Bid-ToReplace-2"),
                IncomingLimitOrder(0.1, 7800.0, "NotFoundPrevious-2", oldUid = "NotExist-2"),
                IncomingLimitOrder(0.05, 7700.0, "bid4")
        ), cancel = true))

        assertOrderBookSize("BTCUSD", true, 3)
        assertOrderBookSize("BTCUSD", false, 3)

        assertBalance("Client1", "BTC", 1.1, 0.7)
        assertBalance("Client1", "USD", 2200.0, 1255.0)

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().first() as LimitOrdersReport

        assertEquals(17, report.orders.size)

        val replacedOrders = report.orders.filter { it.order.status == OrderStatus.Replaced.name }
        assertEquals(4, replacedOrders.size)
        assertTrue(listOf("Ask-ToReplace-1", "Ask-ToReplace-2", "Bid-ToReplace-1", "Bid-ToReplace-2")
                .containsAll(replacedOrders.map { it.order.externalId }))

        val notFoundPreviousOrders = report.orders.filter { it.order.status == OrderStatus.NotFoundPrevious.name }
        assertEquals(2, notFoundPreviousOrders.size)
        assertTrue(listOf("NotFoundPrevious-1", "NotFoundPrevious-2").containsAll(notFoundPreviousOrders.map { it.order.externalId }))

        val notEnoughFundsOrders = report.orders.filter { it.order.status == OrderStatus.NotEnoughFunds.name }
        assertEquals(1, notEnoughFundsOrders.size)
        assertTrue(listOf("NotEnoughFunds").containsAll(notEnoughFundsOrders.map { it.order.externalId }))

        val matchedOrders = report.orders.filter { it.order.status == OrderStatus.Matched.name }
        assertEquals(1, matchedOrders.size)
        assertTrue(listOf("ClientOrder").containsAll(matchedOrders.map { it.order.externalId }))

        val processedOrders = report.orders.filter { it.order.status == OrderStatus.Processing.name }
        assertEquals(1, processedOrders.size)
        assertTrue(listOf("bid1").containsAll(processedOrders.map { it.order.externalId }))

        val inOrderBookOrders = report.orders.filter { it.order.status == OrderStatus.InOrderBook.name }
        assertEquals(5, inOrderBookOrders.size)
        assertTrue(listOf("ask2", "ask3", "ask4", "bid2", "bid4").containsAll(inOrderBookOrders.map { it.order.externalId }))

        val cancelledOrders = report.orders.filter { it.order.status == OrderStatus.Cancelled.name }
        assertEquals(3, cancelledOrders.size)
        assertTrue(listOf("Ask-ToCancel-1", "Ask-ToCancel-2", "Bid-ToCancel-1").containsAll(cancelledOrders.map { it.order.externalId }))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(17, event.orders.size)

        val eventReplacedOrders = event.orders.filter { it.status == OutgoingOrderStatus.REPLACED }
        assertEquals(4, eventReplacedOrders.size)
        assertTrue(listOf("Ask-ToReplace-1", "Ask-ToReplace-2", "Bid-ToReplace-1", "Bid-ToReplace-2")
                .containsAll(eventReplacedOrders.map { it.externalId }))

        val eventNotFoundPreviousOrders = event.orders.filter { it.status == OutgoingOrderStatus.REJECTED && it.rejectReason == OrderRejectReason.NOT_FOUND_PREVIOUS }
        assertEquals(2, eventNotFoundPreviousOrders.size)
        assertTrue(listOf("NotFoundPrevious-1", "NotFoundPrevious-2").containsAll(eventNotFoundPreviousOrders.map { it.externalId }))

        val eventNotEnoughFundsOrders = event.orders.filter { it.status == OutgoingOrderStatus.REJECTED && it.rejectReason == OrderRejectReason.NOT_ENOUGH_FUNDS }
        assertEquals(1, eventNotEnoughFundsOrders.size)
        assertTrue(listOf("NotEnoughFunds").containsAll(eventNotEnoughFundsOrders.map { it.externalId }))

        val evevntMatchedOrders = event.orders.filter { it.status == OutgoingOrderStatus.MATCHED }
        assertEquals(1, evevntMatchedOrders.size)
        assertTrue(listOf("ClientOrder").containsAll(evevntMatchedOrders.map { it.externalId }))

        val eventProcessedOrders = event.orders.filter { it.status == OutgoingOrderStatus.PARTIALLY_MATCHED }
        assertEquals(1, eventProcessedOrders.size)
        assertTrue(listOf("bid1").containsAll(eventProcessedOrders.map { it.externalId }))

        val eventInOrderBookOrders = event.orders.filter { it.status == OutgoingOrderStatus.PLACED }
        assertEquals(5, eventInOrderBookOrders.size)
        assertTrue(listOf("ask2", "ask3", "ask4", "bid2", "bid4").containsAll(eventInOrderBookOrders.map { it.externalId }))

        val eventCancelledOrders = event.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }
        assertEquals(3, eventCancelledOrders.size)
        assertTrue(listOf("Ask-ToCancel-1", "Ask-ToCancel-2", "Bid-ToCancel-1").containsAll(eventCancelledOrders.map { it.externalId }))
    }


    @Test
    fun testAddLimitOrderWithSameReserveSum() {
        //Do not send balance update if balances didn't change
        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("EURUSD", "Client2", listOf(
                IncomingLimitOrder(100.0, 1.2, "1"),
                IncomingLimitOrder(100.0, 1.3, "2")
        )))

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(250.0), testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))

        assertEquals(1, testClientLimitOrderListener.getCount())
        var limitOrders = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf(1.2), limitOrders.orders[0].order.price)
        assertEquals(BigDecimal.valueOf(1.3), limitOrders.orders[1].order.price)
        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals("1.2", event.orders[0].price)
        assertEquals("1.3", event.orders[1].price)
        assertEquals(1, event.balanceUpdates?.size)


        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("EURUSD", "Client2", listOf(
                IncomingLimitOrder(100.0, 1.2, "3", oldUid = "1"),
                IncomingLimitOrder(100.0, 1.3, "4", oldUid = "2")
        )))

        assertEquals(1, testClientLimitOrderListener.getCount())
        limitOrders = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(4, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf(1.2), limitOrders.orders[2].order.price)
        assertEquals(BigDecimal.valueOf(1.3), limitOrders.orders[3].order.price)
        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(4, event.orders.size)
        assertEquals("1.2", event.orders[2].price)
        assertEquals("1.3", event.orders[3].price)
        assertEquals(0, event.balanceUpdates?.size)
    }

    @Test
    fun testReplaceOrderWithAnotherPairAndClient() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "orderWithAnotherPair", assetId = "EURUSD", clientId = "Client1",
                volume = 1.0, price = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "orderWithAnotherClient", assetId = "BTCUSD", clientId = "Client2",
                volume = 1.0, price = 1.0))

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(pair = "BTCUSD", clientId = "Client1",
                orders = listOf(IncomingLimitOrder(oldUid = "orderWithAnotherPair", volume = 1.1, price = 1.0),
                        IncomingLimitOrder(oldUid = "orderWithAnotherClient", volume = 1.1, price = 1.0)), cancel = false))

        assertOrderBookSize("EURUSD", true, 1)
        assertOrderBookSize("BTCUSD", true, 1)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders[0].status)
        assertEquals(OrderRejectReason.NOT_FOUND_PREVIOUS, event.orders[0].rejectReason)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders[1].status)
        assertEquals(OrderRejectReason.NOT_FOUND_PREVIOUS, event.orders[1].rejectReason)
    }

    @Test
    fun testComplexMultiLimitOrder() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 100.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 100.0)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 2.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 2.0)

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 0.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "USD", 0.0)

        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 0.0)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "BTC", 0.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 730.0)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "USD", 730.0)

        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 2.0)
        testBalanceHolderWrapper.updateReservedBalance("Client4", "BTC", 2.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 530.0)
        testBalanceHolderWrapper.updateReservedBalance("Client4", "USD", 301.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 1.0, price = 100.0))

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -1.0, price = 220.0, uid = "LimitOrder-1", reservedVolume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -1.0, price = 206.0, uid = "LimitOrder-2", reservedVolume = 1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "BTCUSD", volume = -1.0, price = 200.0, uid = "LimitOrder-3", reservedVolume = 1.0))

        testOrderBookWrapper.addStopLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", uid = "StopOrder-1",
                volume = -1.0, type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 102.0, upperPrice = 101.0, reservedVolume = 1.0))
        testOrderBookWrapper.addStopLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "BTCUSD", uid = "StopOrder-4",
                volume = -1.0, type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 110.0, upperPrice = 110.0, reservedVolume = 1.0))

        testOrderBookWrapper.addStopLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", uid = "StopOrder-2",
                volume = 1.0, type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 202.0, upperPrice = 210.0, reservedVolume = 210.0))
        testOrderBookWrapper.addStopLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", uid = "StopOrder-3",
                volume = 1.0, type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 210.0, upperPrice = 220.0, reservedVolume = 220.0))
        testOrderBookWrapper.addStopLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD",
                volume = 1.0, type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 300.0, upperPrice = 300.0))
        testOrderBookWrapper.addStopLimitOrder(buildLimitOrder(clientId = "Client4", assetId = "BTCUSD", uid = "ToReplace",
                volume = 1.0, type = LimitOrderType.STOP_LIMIT,
                lowerLimitPrice = 1.0, lowerPrice = 1.0, upperLimitPrice = 301.0, upperPrice = 301.0, reservedVolume = 301.0))

        initServices()

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper(clientId = "Client4", pair = "BTCUSD",
                orders = listOf(IncomingLimitOrder(volume = 2.0, price = -4.0, uid = "IncomingInvalidPrice"),
                        IncomingLimitOrder(volume = 2.0, type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 206.0, upperPrice = 210.0, uid = "Incoming-1", oldUid = "ToReplace"),
                        IncomingLimitOrder(volume = 0.5, price = 220.0, uid = "Incoming-2")), cancel = true, cancelMode = OrderCancelMode.SELL_SIDE))

        assertStopOrderBookSize("BTCUSD", true, 1)
        assertStopOrderBookSize("BTCUSD", false, 0)
        assertOrderBookSize("BTCUSD", true, 3)
        assertOrderBookSize("BTCUSD", false, 0)

        assertBalance("Client1", "BTC", 0.0, 0.0)
        assertBalance("Client1", "USD", 526.0, 100.0)
        assertBalance("Client2", "BTC", 0.0, 0.0)
        assertBalance("Client2", "USD", 210.0, 0.0)
        assertBalance("Client3", "BTC", 0.5, 0.0)
        assertBalance("Client3", "USD", 620.0, 620.0)
        assertBalance("Client4", "BTC", 4.5, 0.0)
        assertBalance("Client4", "USD", 4.0, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent

        assertEquals(8, event.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "BTC", "2", "0", "2", "0", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client1", "USD", "100", "526", "100", "100", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client2", "BTC", "1", "0", "1", "0", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client2", "USD", "0", "210", "0", "0", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client3", "BTC", "0", "0.5", "0", "0", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client3", "USD", "730", "620", "730", "620", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client4", "BTC", "2", "4.5", "2", "0", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client4", "USD", "530", "4", "301", "0", event.balanceUpdates!!)

        assertEquals(15, event.orders.size)

        val stopOrder1Child = event.orders.single { it.parentExternalId == "StopOrder-1" }
        val stopOrder2Child = event.orders.single { it.parentExternalId == "StopOrder-2" }
        val stopOrder3Child = event.orders.single { it.parentExternalId == "StopOrder-3" }
        val incoming1Child = event.orders.single { it.parentExternalId == "Incoming-1" }

        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "LimitOrder-3" }.status)
        assertEquals(0, event.orders.single { it.externalId == "LimitOrder-3" }.trades?.size)

        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "StopOrder-4" }.status)
        assertEquals(0, event.orders.single { it.externalId == "StopOrder-4" }.trades?.size)

        assertEquals(OutgoingOrderStatus.REPLACED, event.orders.single { it.externalId == "ToReplace" }.status)
        assertEquals(0, event.orders.single { it.externalId == "ToReplace" }.trades?.size)

        var order = event.orders.single { it.externalId == "LimitOrder-1" }
        assertEquals(OutgoingOrderStatus.MATCHED, order.status)
        assertEquals(2, order.trades?.size)
        assertEquals("Incoming-2", order.trades!!.single { it.index == 1 }.oppositeExternalOrderId)
        assertEquals(stopOrder3Child.externalId, order.trades!!.single { it.index == 3 }.oppositeExternalOrderId)

        order = event.orders.single { it.externalId == "LimitOrder-2" }
        assertEquals(OutgoingOrderStatus.MATCHED, order.status)
        assertEquals(1, order.trades?.size)
        assertEquals(incoming1Child.externalId, order.trades!!.single { it.index == 0 }.oppositeExternalOrderId)

        order = event.orders.single { it.externalId == "StopOrder-1" }
        assertEquals(OutgoingOrderStatus.EXECUTED, order.status)
        assertEquals(stopOrder1Child.externalId, order.childExternalId)
        assertEquals(0, order.trades?.size)

        order = stopOrder1Child
        assertEquals(OutgoingOrderStatus.MATCHED, order.status)
        assertEquals(1, order.trades?.size)
        assertEquals(incoming1Child.externalId, order.trades!!.single { it.index == 2 }.oppositeExternalOrderId)

        order = event.orders.single { it.externalId == "StopOrder-2" }
        assertEquals(OutgoingOrderStatus.EXECUTED, order.status)
        assertEquals(stopOrder2Child.externalId, order.childExternalId)
        assertEquals(0, order.trades?.size)

        order = stopOrder2Child
        assertEquals(OutgoingOrderStatus.PLACED, order.status)
        assertEquals(0, order.trades?.size)

        order = event.orders.single { it.externalId == "StopOrder-3" }
        assertEquals(OutgoingOrderStatus.EXECUTED, order.status)
        assertEquals(stopOrder3Child.externalId, order.childExternalId)
        assertEquals(0, order.trades?.size)

        order = stopOrder3Child
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, order.status)
        assertEquals(1, order.trades?.size)
        assertEquals("LimitOrder-1", order.trades!!.single { it.index == 3 }.oppositeExternalOrderId)

        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single { it.externalId == "IncomingInvalidPrice" }.status)
        assertEquals(OrderRejectReason.INVALID_PRICE, event.orders.single { it.externalId == "IncomingInvalidPrice" }.rejectReason)
        assertEquals(0, event.orders.single { it.externalId == "IncomingInvalidPrice" }.trades?.size)

        order = event.orders.single { it.externalId == "Incoming-1" }
        assertEquals(OutgoingOrderStatus.EXECUTED, order.status)
        assertEquals(incoming1Child.externalId, order.childExternalId)
        assertEquals(0, order.trades?.size)

        order = incoming1Child
        assertEquals(OutgoingOrderStatus.MATCHED, order.status)
        assertEquals(2, order.trades?.size)
        assertEquals("LimitOrder-2", order.trades!!.single { it.index == 0 }.oppositeExternalOrderId)
        assertEquals(stopOrder1Child.externalId, order.trades!!.single { it.index == 2 }.oppositeExternalOrderId)

        order = event.orders.single { it.externalId == "Incoming-2" }
        assertEquals(OutgoingOrderStatus.MATCHED, order.status)
        assertEquals(1, order.trades?.size)
        assertEquals("LimitOrder-1", order.trades!!.single { it.index == 1 }.oppositeExternalOrderId)

    }

    @Test
    fun testCancelPartiallyMatchedOrderAfterRejectedIncomingOrder() {
        testSettingDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("TrustedClient"))
        applicationSettingsCache.update()

        testBalanceHolderWrapper.updateBalance("Client1", "USD", 0.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 0.0)

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 5.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 5.0)

        testBalanceHolderWrapper.updateBalance("TrustedClient", "EUR", 4.0)

        testBalanceHolderWrapper.updateBalance("Client2", "USD", 100.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "TrustedClient", assetId = "EURUSD", volume = -9.0, price = 1.1))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", volume = -5.0, price = 1.2))

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("EURUSD", "Client2",
                listOf(IncomingLimitOrder(volume = 1.0, price = 1.4, uid = "matched1"),
                        IncomingLimitOrder(volume = 3.0, price = 1.3, uid = "matched2"),
                        IncomingLimitOrder(volume = 5.0, price = 1.2, uid = "rejectedAfterMatching",
                                // 'not enough funds' fee to cancel this order during matching
                                feeInstructions = listOf(NewLimitOrderFeeInstruction(FeeType.CLIENT_FEE, FeeSizeType.PERCENTAGE, BigDecimal.valueOf(0.1), null, null, null, "FeeTargetClient", listOf("BTC"), null))
                        ))))


        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 0)

        assertBalance("Client1", "EUR", 5.0, 5.0)
        assertBalance("Client1", "USD", 0.0, 0.0)

        assertBalance("Client2", "EUR", 4.0, 0.0)
        assertBalance("Client2", "USD", 95.6, 0.0)

        assertBalance("TrustedClient", "EUR", 0.0, 0.0)
        assertBalance("TrustedClient", "USD", 4.4, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent

        assertEquals(4, event.orders.size)
        assertEquals(4, event.balanceUpdates?.size)

        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.externalId == "matched1" }.status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.externalId == "matched2" }.status)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single { it.externalId == "rejectedAfterMatching" }.status)

        val trustedClientOrder = event.orders.single { it.walletId == "TrustedClient" }
        assertEquals(OutgoingOrderStatus.CANCELLED, trustedClientOrder.status)
        assertEquals(2, trustedClientOrder.trades?.size)
        assertEquals("-5", trustedClientOrder.remainingVolume)
    }

    @Test
    fun testCancelPartiallyMatchedOrderDueToNotEnoughFunds() {
        testSettingDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("TrustedClient"))
        applicationSettingsCache.update()

        testBalanceHolderWrapper.updateBalance("Client1", "USD", 0.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 0.0)

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 8.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 8.0)

        testBalanceHolderWrapper.updateBalance("TrustedClient", "EUR", 5.0)

        testBalanceHolderWrapper.updateBalance("Client2", "USD", 100.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "TrustedClient", assetId = "EURUSD", volume = -9.0, price = 1.1))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", volume = -5.0, price = 1.2))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", volume = -3.0, price = 1.4))

        multiLimitOrderService.processMessage(messageBuilder.buildMultiLimitOrderWrapper("EURUSD", "Client2",
                listOf(IncomingLimitOrder(volume = 1.0, price = 1.4),
                        IncomingLimitOrder(volume = 3.0, price = 1.3),
                        IncomingLimitOrder(volume = 5.0, price = 1.2))))

        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 0)

        assertBalance("Client1", "EUR", 3.0, 3.0)
        assertBalance("Client1", "USD", 6.0, 0.0)

        assertBalance("Client2", "EUR", 9.0, 0.0)
        assertBalance("Client2", "USD", 89.6, 0.0)

        assertBalance("TrustedClient", "EUR", 1.0, 0.0)
        assertBalance("TrustedClient", "USD", 4.4, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent

        assertEquals(5, event.orders.size)
        assertEquals(6, event.balanceUpdates?.size)

        assertEquals(3, event.orders.filter { it.walletId == "Client2" }.size)
        event.orders.filter { it.walletId == "Client2" }.forEach {
            assertEquals(OutgoingOrderStatus.MATCHED, it.status)
        }

        val trustedClientOrder = event.orders.single { it.walletId == "TrustedClient" }
        assertEquals(OutgoingOrderStatus.CANCELLED, trustedClientOrder.status)
        assertEquals(2, trustedClientOrder.trades?.size)
        assertEquals("-5", trustedClientOrder.remainingVolume)

        val clientOrder = event.orders.single { it.walletId == "Client1" }
        assertEquals(OutgoingOrderStatus.MATCHED, clientOrder.status)
    }
}