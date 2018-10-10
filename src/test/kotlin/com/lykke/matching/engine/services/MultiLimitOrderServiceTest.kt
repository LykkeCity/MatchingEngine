package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import com.lykke.matching.engine.utils.NumberUtils
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
@SpringBootTest(classes = [(TestApplicationContext::class), (MultiLimitOrderServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MultiLimitOrderServiceTest: AbstractTest() {

    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("TIME", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("LKK1Y", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("LKK", 2))

            return testBackOfficeDatabaseAccessor
        }


        @Bean
        @Primary
        open fun testConfig(): TestSettingsDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
            testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("Client1"))
            testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("Client5"))
            return testSettingsDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("TIMEUSD", "TIME", "USD", 6))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("LKK1YLKK", "LKK1Y", "LKK", 5))

        initServices()
    }

    @Test
    fun testSmallVolume() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)))
        
        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1",
                orders = listOf(
                        IncomingLimitOrder(0.1, 2.0),
                        IncomingLimitOrder(0.1, 1.5),
                        IncomingLimitOrder(0.09, 1.3),
                        IncomingLimitOrder(1.0, 1.2),
                        IncomingLimitOrder(-1.0, 2.1),
                        IncomingLimitOrder(-0.09, 2.2),
                        IncomingLimitOrder(-0.1, 2.4)
                )))
        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        val limitOrders = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(5, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf( 2.0), limitOrders.orders[0].order.price)
        assertEquals(BigDecimal.valueOf(1.5), limitOrders.orders[1].order.price)
        assertEquals(BigDecimal.valueOf(1.2), limitOrders.orders[2].order.price)
        assertEquals(BigDecimal.valueOf(2.1), limitOrders.orders[3].order.price)
        assertEquals(BigDecimal.valueOf(2.4), limitOrders.orders[4].order.price)

        assertEquals(0, clientsEventsQueue.size)
        assertEquals(1, trustedClientsEventsQueue.size)
        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(5, event.orders.size)
        assertEquals("2", event.orders[0].price)
        assertEquals("1.5", event.orders[1].price)
        assertEquals("1.2", event.orders[2].price)
        assertEquals("2.1", event.orders[3].price)
        assertEquals("2.4", event.orders[4].price)
    }

    @Test
    fun testAddLimitOrder() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(100.0, 1.2), IncomingLimitOrder(100.0, 1.3))))

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        val limitOrders = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf(1.2), limitOrders.orders[0].order.price)
        assertEquals(BigDecimal.valueOf(1.3), limitOrders.orders[1].order.price)

        assertEquals(0, clientsEventsQueue.size)
        assertEquals(1, trustedClientsEventsQueue.size)
        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals("1.2", event.orders[0].price)
        assertEquals("1.3", event.orders[1].price)

        assertEquals(1, tradesInfoListener.getCount())
        val tradeInfo = tradesInfoListener.getProcessingQueue().poll()
        assertEquals(BigDecimal.valueOf(1.3), tradeInfo.price)
        assertEquals(true, tradeInfo.isBuy)
    }

    @Test
    fun testAdd2LimitOrder() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(100.0, 1.2), IncomingLimitOrder(100.0, 1.3))))

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        var limitOrders = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf(1.2), limitOrders.orders[0].order.price)
        assertEquals(BigDecimal.valueOf(1.3), limitOrders.orders[1].order.price)

        assertEquals(1, trustedClientsEventsQueue.size)
        var event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals("1.2", event.orders[0].price)
        assertEquals("1.3", event.orders[1].price)


        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(100.0, 1.4), IncomingLimitOrder(100.0, 1.5)),
                cancel = false))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        limitOrders = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf(1.4), limitOrders.orders[0].order.price)
        assertEquals(BigDecimal.valueOf(1.5), limitOrders.orders[1].order.price)

        assertEquals(1, trustedClientsEventsQueue.size)
        event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals("1.4", event.orders[0].price)
        assertEquals("1.5", event.orders[1].price)
    }

    @Test
    fun testAddAndCancelLimitOrder() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(100.0, 1.2), IncomingLimitOrder(100.0, 1.3))))

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        var limitOrders = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf(1.2), limitOrders.orders[0].order.price)
        assertEquals(BigDecimal.valueOf(1.3), limitOrders.orders[1].order.price)
        assertOrderBookSize("EURUSD", true, 2)

        assertEquals(1, trustedClientsEventsQueue.size)
        var event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals("1.2", event.orders[0].price)
        assertEquals("1.3", event.orders[1].price)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(100.0, 1.4), IncomingLimitOrder(100.0, 1.5)),
                cancel = false))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        limitOrders = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf(1.4), limitOrders.orders[0].order.price)
        assertEquals(BigDecimal.valueOf(1.5), limitOrders.orders[1].order.price)
        assertOrderBookSize("EURUSD", true, 4)

        assertEquals(1, trustedClientsEventsQueue.size)
        event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals("1.4", event.orders[0].price)
        assertEquals("1.5", event.orders[1].price)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(100.0, 2.0), IncomingLimitOrder(100.0, 2.1)), cancel = true))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        limitOrders = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(6, limitOrders.orders.size)
        assertEquals(BigDecimal.valueOf(1.2), limitOrders.orders[0].order.price)
        assertEquals(BigDecimal.valueOf(1.3), limitOrders.orders[1].order.price)
        assertEquals(BigDecimal.valueOf(1.4), limitOrders.orders[2].order.price)
        assertEquals(BigDecimal.valueOf(1.5), limitOrders.orders[3].order.price)
        assertEquals(BigDecimal.valueOf(2.0), limitOrders.orders[4].order.price)
        assertEquals(BigDecimal.valueOf(2.1), limitOrders.orders[5].order.price)
        assertOrderBookSize("EURUSD", true, 2)

        assertEquals(1, trustedClientsEventsQueue.size)
        event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(6, event.orders.size)
        assertEquals("1.2", event.orders[0].price)
        assertEquals("1.3", event.orders[1].price)
        assertEquals("1.4", event.orders[2].price)
        assertEquals("1.5", event.orders[3].price)
        assertEquals("2", event.orders[4].price)
        assertEquals("2.1", event.orders[5].price)
    }

    @Test
    fun testAddAndMatchLimitOrder() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(100.0, 1.3), IncomingLimitOrder(100.0, 1.2))))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        testTrustedClientsLimitOrderListener.clear()
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", price = 1.25, volume = -150.0)))

        assertEquals(1, testClientLimitOrderListener.getCount())
        var result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport

        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(BigDecimal.valueOf(-50.0), result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(1.3), result.orders[1].order.price)

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals("-50", event.orders[0].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals("1.3", event.orders[1].price)

        assertEquals(BigDecimal.valueOf(870.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(1100.0), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(BigDecimal.valueOf(1130.0), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(900.0), testWalletDatabaseAccessor.getBalance("Client2", "EUR"))
        assertEquals(BigDecimal.valueOf(50.0), testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))
        assertOrderBookSize("EURUSD", true, 1)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(10.0, 1.3), IncomingLimitOrder(100.0, 1.26),
                IncomingLimitOrder(100.0, 1.2)), cancel = true))

        assertEquals(1, testClientLimitOrderListener.getCount())
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport

        assertEquals(3, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(BigDecimal.ZERO, result.orders[0].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(1.3), result.orders[0].order.price)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(1.25), result.orders[1].order.price)
        assertEquals(OrderStatus.Processing.name, result.orders[2].order.status)
        assertEquals(BigDecimal.valueOf(60.0), result.orders[2].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(1.26), result.orders[2].order.price)

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals("0", event.orders[0].remainingVolume)
        assertEquals("1.3", event.orders[0].price)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals("1.25", event.orders[1].price)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[2].status)
        assertEquals("60", event.orders[2].remainingVolume)
        assertEquals("1.26", event.orders[2].price)

        assertEquals(BigDecimal.valueOf(807.5), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(1150.0), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(BigDecimal.valueOf(1192.5), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(850.0), testWalletDatabaseAccessor.getBalance("Client2", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))
        assertOrderBookSize("EURUSD", true, 2)

    }

    @Test
    fun testAddAndMatchLimitOrder2() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(-100.0, 1.2), IncomingLimitOrder(-100.0, 1.3))))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        testTrustedClientsLimitOrderListener.getQueue().poll()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", price = 1.25, volume = 150.0)))

        assertEquals(1, testClientLimitOrderListener.getCount())
        var result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport

        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(BigDecimal.valueOf(50.0), result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(1.2), result.orders[1].order.price)

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals("50", event.orders[0].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals("1.2", event.orders[1].price)

        assertEquals(BigDecimal.valueOf(1120.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(900.0), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(BigDecimal.valueOf(880.0), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(1100.0), testWalletDatabaseAccessor.getBalance("Client2", "EUR"))
        assertEquals(BigDecimal.valueOf(62.5), testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(-10.0, 1.2),
                IncomingLimitOrder(-10.0, 1.24),
                IncomingLimitOrder(-10.0, 1.29),
                IncomingLimitOrder(-10.0, 1.3)), cancel = true))

        assertEquals(1, testClientLimitOrderListener.getCount())
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport

        assertEquals(3, result.orders.size)
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(BigDecimal.ZERO, result.orders[0].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(1.2), result.orders[0].order.price)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(30.0), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(1.25), result.orders[1].order.price)
        assertEquals(OrderStatus.Matched.name, result.orders[2].order.status)
        assertEquals(BigDecimal.ZERO, result.orders[2].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(1.24), result.orders[2].order.price)

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals("0", event.orders[0].remainingVolume)
        assertEquals("1.2", event.orders[0].price)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("30", event.orders[1].remainingVolume)
        assertEquals("1.25", event.orders[1].price)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[2].status)
        assertEquals("0", event.orders[2].remainingVolume)
        assertEquals("1.24", event.orders[2].price)

        assertEquals(BigDecimal.valueOf(1.25), genericLimitOrderService.getOrderBook("EURUSD").getBidPrice())

        assertEquals(BigDecimal.valueOf(1145.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(880.0), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(BigDecimal.valueOf(855.0), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(1120.0), testWalletDatabaseAccessor.getBalance("Client2", "EUR"))
        assertEquals(BigDecimal.valueOf(37.5), testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrder3() {
        testBalanceHolderWrapper.updateBalance("Client5", "USD", 18.6)
        testBalanceHolderWrapper.updateBalance("Client5", "TIME", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "TIME", 1000.0)

        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-100.0, 26.955076))))
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(0.69031943, 26.915076))))

        assertEquals(2, testTrustedClientsLimitOrderListener.getCount())
        testTrustedClientsLimitOrderListener.getQueue().poll()
        testTrustedClientsLimitOrderListener.getQueue().poll()

        assertEquals(2, trustedClientsEventsQueue.size)
        clearMessageQueues()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(assetId = "TIMEUSD", clientId = "Client2", price = 26.88023, volume = -26.0)))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport

        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(BigDecimal.valueOf(-25.30968057), result.orders[0].order.remainingVolume)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(26.915076), result.orders[1].order.price)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals("-25.30968057", event.orders[0].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals("26.915076", event.orders[1].price)

        var orderBook = genericLimitOrderService.getOrderBook("TIMEUSD")
        assertEquals(2, orderBook.getOrderBook(false).size)
        var bestAskOrder = orderBook.getOrderBook(false).peek()
        assertEquals(BigDecimal.valueOf(26.88023), bestAskOrder.price)
        assertEquals(BigDecimal.valueOf(-26.0), bestAskOrder.volume)
        assertEquals(BigDecimal.valueOf(-25.30968057), bestAskOrder.remainingVolume)

        assertEquals(0, orderBook.getOrderBook(true).size)

        assertEquals(BigDecimal.valueOf(0.03), testWalletDatabaseAccessor.getBalance("Client5", "USD"))
        assertEquals(BigDecimal.valueOf(1000.69031943), testWalletDatabaseAccessor.getBalance("Client5", "TIME"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client5", "USD"))

        assertEquals(BigDecimal.valueOf(1018.57), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(999.30968057), testWalletDatabaseAccessor.getBalance("Client2", "TIME"))
        assertEquals(BigDecimal.valueOf(25.30968057), testWalletDatabaseAccessor.getReservedBalance("Client2", "TIME"))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "TIMEUSD", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(10.0, 26.915076), IncomingLimitOrder(10.0, 26.875076)), cancel = true))

        assertEquals(0, testClientLimitOrderListener.getCount())
        assertEquals(0, clientsEventsQueue.size)

        orderBook = genericLimitOrderService.getOrderBook("TIMEUSD")
        assertEquals(2, orderBook.getOrderBook(false).size)
        bestAskOrder = orderBook.getOrderBook(false).peek()
        assertEquals(BigDecimal.valueOf(26.88023), bestAskOrder.price)
        assertEquals(BigDecimal.valueOf(-26.0), bestAskOrder.volume)
        assertEquals(BigDecimal.valueOf(-25.30968057), bestAskOrder.remainingVolume)

        assertEquals(0, orderBook.getOrderBook(true).size)
    }

    @Test
    fun testAddAndMatchLimitOrderZeroVolumes() {
        testBalanceHolderWrapper.updateBalance("Client5", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCEUR", clientId = "Client2", price = 3629.355, volume = 0.19259621)))

        assertEquals(1, testClientLimitOrderListener.getCount())
        var result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport

        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)
        assertEquals(BigDecimal.valueOf(0.19259621), result.orders[0].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(699.01), result.orders[0].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(699.01), testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)
        assertEquals("0.19259621", event.orders[0].remainingVolume)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.00574996, 3628.707)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(0.18684625), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(678.15), result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(678.15), testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("0.18684625", event.orders[1].remainingVolume)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.01431186, 3624.794),
                IncomingLimitOrder(-0.02956591, 3626.591)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(0.14296848), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(518.91), result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(518.91), testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))
        assertEquals(OrderStatus.Matched.name, result.orders[2].order.status)

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("0.14296848", event.orders[1].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[2].status)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.04996673, 3625.855)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(0.09300175), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(337.57), result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(337.57), testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("0.09300175", event.orders[1].remainingVolume)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.00628173, 3622.865),
                IncomingLimitOrder(-0.01280207, 3625.489),
                IncomingLimitOrder(-0.02201331, 3627.41),
                IncomingLimitOrder(-0.02628901, 3629.139)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(0.02561563), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(93.02), result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(93.02), testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))
        assertEquals(OrderStatus.Matched.name, result.orders[2].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[3].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[4].order.status)

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("0.02561563", event.orders[1].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[2].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[3].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[4].status)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.01708411, 3626.11)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(0.00853152), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(31.02), result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(31.02), testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("0.00853152", event.orders[1].remainingVolume)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = "Client5", orders =
        listOf(IncomingLimitOrder(-0.00959341, 3625.302)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)
        assertEquals(BigDecimal.ZERO, result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.ZERO, result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals("0", event.orders[1].remainingVolume)

        val orderBook = genericLimitOrderService.getOrderBook("BTCEUR")
        assertEquals(1, orderBook.getOrderBook(false).size)
        val bestAskOrder = orderBook.getOrderBook(false).peek()
        assertEquals(BigDecimal.valueOf(3625.302), bestAskOrder.price)
        assertEquals(BigDecimal.valueOf(-0.00959341), bestAskOrder.volume)
        assertEquals(BigDecimal.valueOf(-0.00106189), bestAskOrder.remainingVolume)

        assertEquals(0, orderBook.getOrderBook(true).size)
    }

    @Test
    fun testAddAndMatchAndCancel() {
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("Client3"))

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.26170853)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC",  0.001)
        testBalanceHolderWrapper.updateBalance("Client3", "CHF", 1000.0)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCCHF", uid = "1", price = 4384.15, volume = -0.26070853)))

        assertEquals(1, testClientLimitOrderListener.getCount())
        var result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)
        assertEquals(BigDecimal.valueOf(0.26170853), testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(BigDecimal.valueOf(0.26170853), testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCCHF", clientId = "Client3", orders =
        listOf(IncomingLimitOrder(0.00643271, 4390.84),
                IncomingLimitOrder(0.01359005, 4387.87),
                IncomingLimitOrder(0.02033985, 4384.811)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(-0.22034592), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(0.22034592), result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(0.22134592), testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))
        assertEquals(OrderStatus.Matched.name, result.orders[2].order.status)
        assertEquals(OrderStatus.Matched.name, result.orders[3].order.status)

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("-0.22034592", event.orders[1].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[2].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[3].status)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCCHF", clientId = "Client3", orders =
        listOf(IncomingLimitOrder(0.01691068, 4387.21)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(-0.20343524), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(0.20343524), result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(0.20443524), testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("-0.20343524", event.orders[1].remainingVolume)

        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper("1"))
        assertEquals(BigDecimal.valueOf(0.001), testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))
    }

    @Test
    fun testBalance() {
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.26170853)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC",  0.001)
        testBalanceHolderWrapper.updateBalance("Client3", "CHF", 100.0)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCCHF", uid = "1", price = 4384.15, volume = -0.26070853)))

        assertEquals(1, testClientLimitOrderListener.getCount())
        var result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.InOrderBook.name, result.orders[0].order.status)
        assertEquals(BigDecimal.valueOf(0.26170853), testWalletDatabaseAccessor.getBalance("Client2", "BTC"))
        assertEquals(BigDecimal.valueOf(0.26170853), testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCCHF", clientId = "Client3", orders =
        listOf(IncomingLimitOrder(0.00643271, 4390.84),
                IncomingLimitOrder(0.01359005, 4387.87),
                IncomingLimitOrder(0.02033985, 4384.811)), cancel = true))
        result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[0].order.status)
        assertEquals(OrderStatus.Processing.name, result.orders[1].order.status)
        assertEquals(BigDecimal.valueOf(-0.24068577), result.orders[1].order.remainingVolume)
        assertEquals(BigDecimal.valueOf(0.24068577), result.orders[1].order.reservedLimitVolume!!)
        assertEquals(BigDecimal.valueOf(0.24168577), testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))
        assertEquals(OrderStatus.Matched.name, result.orders[2].order.status)

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("-0.24068577", event.orders[1].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[2].status)

        assertEquals(BigDecimal.ZERO, genericLimitOrderService.getOrderBook("BTCCHF").getBidPrice())
        assertEquals(BigDecimal.valueOf(12.2), testWalletDatabaseAccessor.getBalance("Client3", "CHF"))
    }

    @Test
    fun testMatchWithLimitOrderForAllFunds() {
        val marketMaker = "Client1"
        val client = "Client2"

        testBalanceHolderWrapper.updateBalance(client, "EUR", 700.04)
        testBalanceHolderWrapper.updateReservedBalance(client, "EUR",  700.04)
        testBalanceHolderWrapper.updateBalance(marketMaker, "BTC", 2.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = client, assetId = "BTCEUR", price = 4722.0, volume = 0.14825226))
        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "BTCEUR", clientId = marketMaker, orders =
        listOf(IncomingLimitOrder(-0.4435, 4721.403)), cancel = true))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders[1].order.status)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance(client, "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance(client, "EUR"))
        assertEquals(0, genericLimitOrderService.getOrderBook("BTCEUR").getOrderBook(true).size)
    }

    @Test
    fun testFee() {
        val marketMaker = "Client1"
        val client = "Client2"
        val feeHolder = "Client3"

        testBalanceHolderWrapper.updateBalance(client, "EUR", 200.0)
        testBalanceHolderWrapper.updateBalance(marketMaker, "USD", 200.0)
        testBalanceHolderWrapper.updateBalance(marketMaker, "EUR", 0.0)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = client, assetId = "EURUSD", price = 1.2, volume = -50.0)))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(
                pair = "EURUSD",
                clientId = marketMaker,
                orders = listOf(
                        IncomingLimitOrder(60.0, 1.2,
                                feeInstruction = LimitOrderFeeInstruction(FeeType.CLIENT_FEE, FeeSizeType.PERCENTAGE, BigDecimal.valueOf(0.01),
                                        FeeSizeType.PERCENTAGE, BigDecimal.valueOf(0.02), marketMaker, feeHolder)),
                        IncomingLimitOrder(60.0, 1.1,
                                feeInstruction = LimitOrderFeeInstruction(FeeType.CLIENT_FEE, FeeSizeType.PERCENTAGE, BigDecimal.valueOf(0.03),
                                        FeeSizeType.PERCENTAGE, BigDecimal.valueOf(0.04), marketMaker, feeHolder))),
                cancel = true))

        assertEquals(BigDecimal.valueOf(0.5), balancesHolder.getBalance(feeHolder, "EUR")) // 0.01 * 50 (expr1)
        assertEquals(BigDecimal.valueOf(49.5), balancesHolder.getBalance(marketMaker, "EUR")) // 50 - expr1 (expr2)

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = client, assetId = "EURUSD", price = 1.1, volume = -70.0)))

        assertEquals(BigDecimal.valueOf(3.1), balancesHolder.getBalance(feeHolder, "EUR")) // expr1 + 10 * 0.02 + 60 * 0.04 (expr3)
        assertEquals(BigDecimal.valueOf(116.9), balancesHolder.getBalance(marketMaker, "EUR")) // expr2 + 70 - expr3
    }

    @Test
    fun testMatchWithNotEnoughFundsTrustedOrders() {
        val marketMaker = "Client1"
        val client = "Client2"
        testBalanceHolderWrapper.updateBalance(marketMaker, "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 2.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client3", assetId = "EURUSD", price = 1.19, volume = 1.0))

        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(
                clientId = marketMaker, pair = "EURUSD",
                orders = listOf(
                        IncomingLimitOrder(2.0, 1.20),
                        IncomingLimitOrder(2.0, 1.18),
                        IncomingLimitOrder(2.0, 1.15),
                        IncomingLimitOrder(2.0, 1.14),
                        IncomingLimitOrder(2.0, 1.13),
                        IncomingLimitOrder(2.0, 1.1)
                ),
                cancel = true))

        testBalanceHolderWrapper.updateBalance(marketMaker, "USD", 6.0)
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = client, price = 1.15, volume = -5.5)))

        clearMessageQueues()
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = client, price = 1.13, volume = -100.0)))

        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).size)
        assertEquals(BigDecimal.valueOf(1.1), genericLimitOrderService.getOrderBook("EURUSD").getBidPrice())

        assertEquals(1, testClientLimitOrderListener.getCount())
        val trustedResult = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(1, trustedResult.orders.filter { it.order.clientId == marketMaker }.size)

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        val result = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(2, result.orders.filter { it.order.clientId == marketMaker }.size)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(0, (balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate).balances.filter { it.id == marketMaker }.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.filter { it.walletId == marketMaker }.size)
        assertEquals(0, event.balanceUpdates!!.filter { it.walletId == marketMaker }.size)

        assertEquals(1, trustedClientsEventsQueue.size)
        val trustedEvent = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, trustedEvent.orders.filter { it.walletId == marketMaker }.size)
        assertEquals(0, trustedEvent.balanceUpdates!!.size)
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder1() {
        val marketMaker = "Client1"
        val client = "Client2"
        testBalanceHolderWrapper.updateBalance(client, "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(client, "USD",  1.19)

        val order = buildLimitOrder(clientId = client, assetId = "EURUSD", price = 1.2, volume = 1.0)
        order.reservedLimitVolume = BigDecimal.valueOf(1.19)
        testOrderBookWrapper.addLimitOrder(order)

        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(clientId = marketMaker, pair = "EURUSD", orders =
        listOf(IncomingLimitOrder(-2.0, 1.1)), cancel = false))

        assertEquals(0, testOrderDatabaseAccessor.getOrders("EURUSD", true).size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", false).size)
        assertEquals(2, testRabbitOrderBookListener.getCount())

        val orderSell = testOrderDatabaseAccessor.getOrders("EURUSD", false).first()
        assertEquals(BigDecimal.valueOf(-2.0), orderSell.remainingVolume)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance(client, "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance(client, "USD"))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport

        val clientOrderReport = result.orders.filter { it.order.clientId == client }
        assertEquals(1, clientOrderReport.size)
        assertEquals(client, clientOrderReport.first().order.clientId)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate

        assertEquals(1, balanceUpdate.balances.size)
        assertEquals(client, balanceUpdate.balances.first().id)
        assertEquals(BigDecimal.ZERO, balanceUpdate.balances.first().newReserved)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.filter { it.walletId == client }.size)
        assertEquals(1, event.balanceUpdates!!.size)
        assertEquals(client, event.balanceUpdates!!.first().walletId)
        assertEquals("0", event.balanceUpdates!!.first().newReserved)

    }

    @Test
    fun testCancelPreviousOrderWithSameUid() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(-9.0, 0.4875, uid = "orders")),
                cancel = true))

        clearMessageQueues()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(pair = "EURUSD", clientId = "Client1", orders =
        listOf(IncomingLimitOrder(-10.0, 0.4880, uid = "order1")),
                cancel = true))


        assertEquals(BigDecimal.valueOf(-10.0), testOrderDatabaseAccessor.getOrders("EURUSD", false).first().volume)
        assertEquals(OrderStatus.InOrderBook.name, testOrderDatabaseAccessor.getOrders("EURUSD", false).first().status)

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        val result = testTrustedClientsLimitOrderListener.getQueue().first() as LimitOrdersReport

        assertEquals(2, result.orders.size)

        val order = result.orders.first { NumberUtils.equalsIgnoreScale(it.order.volume, BigDecimal.valueOf(-10.0)) }.order
        assertEquals(OrderStatus.InOrderBook.name, order.status)
        assertEquals(BigDecimal.valueOf(0.488), order.price)

        val oldOrder = result.orders.first { NumberUtils.equalsIgnoreScale(it.order.volume, BigDecimal.valueOf(-9.0)) }.order
        assertEquals(OrderStatus.Cancelled.name, oldOrder.status)
        assertEquals(BigDecimal.valueOf(-9.0), oldOrder.volume)
        assertEquals(BigDecimal.valueOf(0.4875), oldOrder.price)

        assertEquals(1, trustedClientsEventsQueue.size)
        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        val eventOrder = event.orders.first { it.volume == "-10" }
        assertEquals(OutgoingOrderStatus.PLACED, eventOrder.status)
        assertEquals("0.488", eventOrder.price)
        val eventOldOrder = event.orders.first { it.volume == "-9" }
        assertEquals(OutgoingOrderStatus.CANCELLED, eventOldOrder.status)
        assertEquals("0.4875", eventOldOrder.price)
    }

    private fun setOrder() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 3000.0)
        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR", "Client1", listOf(
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

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR", "Client1", orders = emptyList(),
                cancel = true, cancelMode = OrderCancelMode.BOTH_SIDES))

        assertOrderBookSize("BTCEUR", true, 0)
        assertOrderBookSize("BTCEUR", false, 0)
        val report = testTrustedClientsLimitOrderListener.getQueue().first() as LimitOrdersReport
        assertEquals(5, report.orders.size)
        report.orders.forEach {
            assertEquals(OrderStatus.Cancelled.name, it.order.status)
        }
        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(5, event.orders.size)
        event.orders.forEach {
            assertEquals(OutgoingOrderStatus.CANCELLED, it.status)
        }
    }

    @Test
    fun testOneSideOrderWithCancelPreviousBothSides() {
        setOrder()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR", "Client1",
                listOf(IncomingLimitOrder(-0.4, 9100.0, "1"),
                        IncomingLimitOrder(-0.3, 9000.0, "2")),
                cancel = true, cancelMode = OrderCancelMode.BOTH_SIDES))

        assertOrderBookSize("BTCEUR", true, 0)
        assertOrderBookSize("BTCEUR", false, 2)
        val report = testTrustedClientsLimitOrderListener.getQueue().first() as LimitOrdersReport
        assertEquals(7, report.orders.size)

        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(7, event.orders.size)

        assertTrue(genericLimitOrderService.getOrderBook("BTCEUR").getOrderBook(false).map { it.externalId }.containsAll(listOf("1", "2")))
    }

    @Test
    fun testBothSidesOrderWithCancelPreviousOneSide() {
        setOrder()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR", "Client1",
                listOf(IncomingLimitOrder(-0.01, 9100.0, "1"),
                        IncomingLimitOrder(-0.009, 9000.0, "2"),
                        IncomingLimitOrder(0.2, 7900.0, "3")),
                cancel = true, cancelMode = OrderCancelMode.BUY_SIDE))

        assertOrderBookSize("BTCEUR", true, 1)
        assertOrderBookSize("BTCEUR", false, 5)
        val report = testTrustedClientsLimitOrderListener.getQueue().first() as LimitOrdersReport
        assertEquals(5, report.orders.size)

        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(5, event.orders.size)

        assertTrue(genericLimitOrderService.getOrderBook("BTCEUR").getOrderBook(true).map { it.externalId } == listOf("3"))
    }

    @Test
    fun testReplaceOrders() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 2.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 3000.0)

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.1)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC",  0.1)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "ClientOrder", clientId = "Client2", assetId = "BTCEUR", volume = -0.1, price = 8000.0))
        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR", "Client1", listOf(
                IncomingLimitOrder(-0.4, 9300.0, "Ask-ToReplace-2"),
                IncomingLimitOrder(-0.3, 9200.0, "Ask-ToReplace-1"),
                IncomingLimitOrder(-0.2, 9100.0, "Ask-ToCancel-2"),
                IncomingLimitOrder(-0.1, 9000.0, "Ask-ToCancel-1"),
                IncomingLimitOrder(0.2, 7900.0, "Bid-ToReplace-1"),
                IncomingLimitOrder(0.1, 7800.0, "Bid-ToCancel-1"),
                IncomingLimitOrder(0.05, 7700.0, "Bid-ToReplace-2")
        )))
        clearMessageQueues()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR", "Client1", listOf(
                IncomingLimitOrder(-0.2, 9400.0, "NotFoundPrevious-1", oldUid = "NotExist-1"),
                IncomingLimitOrder(-0.2, 9300.0, "ask2", oldUid = "Ask-ToReplace-2"),
                IncomingLimitOrder(-0.3, 9200.0, "ask3", oldUid = "Ask-ToReplace-1"),
                IncomingLimitOrder(-0.2, 9100.0, "ask4"),
                IncomingLimitOrder(-0.3001, 9000.0, "ask5"),
                IncomingLimitOrder(0.11, 8000.0, "bid1", oldUid = "Bid-ToReplace-1"),
                IncomingLimitOrder(0.1, 7900.0, "bid2", oldUid = "Bid-ToReplace-2"),
                IncomingLimitOrder(0.1, 7800.0, "NotFoundPrevious-2", oldUid = "NotExist-2"),
                IncomingLimitOrder(0.05, 7700.0, "bid4")
        ), cancel = true))

        assertOrderBookSize("BTCEUR", true, 3)
        assertOrderBookSize("BTCEUR", false, 4)

        assertBalance("Client1", "BTC", 2.1, 0.0)
        assertBalance("Client1", "EUR", 2200.0, 0.0)

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        val trustedReport = testTrustedClientsLimitOrderListener.getQueue().first() as LimitOrdersReport
        assertEquals(13, trustedReport.orders.size)

        val replacedOrders = trustedReport.orders.filter { it.order.status == OrderStatus.Replaced.name }
        assertEquals(4, replacedOrders.size)
        assertTrue(listOf("Ask-ToReplace-1", "Ask-ToReplace-2", "Bid-ToReplace-1", "Bid-ToReplace-2")
                .containsAll(replacedOrders.map { it.order.externalId }))

        val inOrderBookOrders = trustedReport.orders.filter { it.order.status == OrderStatus.InOrderBook.name }
        assertEquals(6, inOrderBookOrders.size)
        assertTrue(listOf("ask2", "ask3", "ask4", "ask5", "bid2", "bid4").containsAll(inOrderBookOrders.map { it.order.externalId }))

        val cancelledOrders = trustedReport.orders.filter { it.order.status == OrderStatus.Cancelled.name }
        assertEquals(3, cancelledOrders.size)
        assertTrue(listOf("Ask-ToCancel-1", "Ask-ToCancel-2", "Bid-ToCancel-1").containsAll(cancelledOrders.map { it.order.externalId }))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val report = testClientLimitOrderListener.getQueue().take()
        assertEquals(2, report.orders.size)

        val matchedOrders = report.orders.filter { it.order.status == OrderStatus.Matched.name }
        assertEquals(1, matchedOrders.size)
        assertTrue(listOf("ClientOrder").containsAll(matchedOrders.map { it.order.externalId }))

        val processedOrders = report.orders.filter { it.order.status == OrderStatus.Processing.name }
        assertEquals(1, processedOrders.size)
        assertTrue(listOf("bid1").containsAll(processedOrders.map { it.order.externalId }))

        assertEquals(1, trustedClientsEventsQueue.size)
        val trustedEvent = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(13, trustedEvent.orders.size)

        val eventReplacedOrders = trustedEvent.orders.filter { it.status == OutgoingOrderStatus.REPLACED }
        assertEquals(4, eventReplacedOrders.size)
        assertTrue(listOf("Ask-ToReplace-1", "Ask-ToReplace-2", "Bid-ToReplace-1", "Bid-ToReplace-2")
                .containsAll(eventReplacedOrders.map { it.externalId }))

        val eventInOrderBookOrders = trustedEvent.orders.filter { it.status == OutgoingOrderStatus.PLACED }
        assertEquals(6, eventInOrderBookOrders.size)
        assertTrue(listOf("ask2", "ask3", "ask4", "ask5", "bid2", "bid4").containsAll(eventInOrderBookOrders.map { it.externalId }))

        val eventCancelledOrders = trustedEvent.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }
        assertEquals(3, eventCancelledOrders.size)
        assertTrue(listOf("Ask-ToCancel-1", "Ask-ToCancel-2", "Bid-ToCancel-1").containsAll(eventCancelledOrders.map { it.externalId }))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)

        val eventMatchedOrders = event.orders.filter { it.status == OutgoingOrderStatus.MATCHED }
        assertEquals(1, eventMatchedOrders.size)
        assertTrue(listOf("ClientOrder").containsAll(eventMatchedOrders.map { it.externalId }))

        val eventProcessedOrders = event.orders.filter { it.status == OutgoingOrderStatus.PARTIALLY_MATCHED }
        assertEquals(1, eventProcessedOrders.size)
        assertTrue(listOf("bid1").containsAll(eventProcessedOrders.map { it.externalId }))
    }

    @Test
    fun testReplaceOrderWithNotEnoughFunds() {
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD", "Client1", listOf(
                IncomingLimitOrder(-100.0, 1.2, "0"),
                IncomingLimitOrder(-400.0, 1.3, "1"),
                IncomingLimitOrder(-400.0, 1.4, "2")
        ), cancel = false))
        clearMessageQueues()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD", "Client1", listOf(
                IncomingLimitOrder(-700.0, 1.3, "3", oldUid = "1"),
                IncomingLimitOrder(-400.0, 1.5, "4", oldUid = "2")
        ), cancel = false))

        assertOrderBookSize("EURUSD", false, 2)
        val orderBook = genericLimitOrderService.getOrderBook("EURUSD")
        assertTrue(orderBook.getOrderBook(false).any { it.externalId == "0" })
        assertTrue(orderBook.getOrderBook(false).any { it.externalId == "3" })

        assertEquals(1, trustedClientsEventsQueue.size)
        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(OutgoingOrderStatus.REPLACED, event.orders.single { it.externalId == "1" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "2" }.status)
        assertEquals(OutgoingOrderStatus.PLACED, event.orders.single { it.externalId == "3" }.status)
    }

    @Test
    fun testCancelPreviousOrderWithSameUidAndMatch() {
        val order = buildLimitOrder(uid = "1",
                assetId = "EURUSD",
                clientId = "Client1",
                volume = 10.0,
                price = 1.2,
                status = OrderStatus.Processing.name)
        order.remainingVolume = BigDecimal.valueOf(9.0) // partially matched
        testOrderBookWrapper.addLimitOrder(order)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "EURUSD",
                clientId = "Client2",
                volume = -10.0,
                price = 1.3,
                status = OrderStatus.Processing.name))

        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD",
                "Client1",
                listOf(IncomingLimitOrder(10.0, 1.3, "1"))))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val result = testClientLimitOrderListener.getQueue().first() as LimitOrdersReport
        assertEquals(3, result.orders.size)

        val orders = result.orders.filter { it.order.externalId == "1" }
        assertEquals(2, orders.size)

        val previousOrderWithTrades = orders.first { it.order.status == OrderStatus.Cancelled.name }
        val newOrderWithTrades = orders.first { it.order.status != OrderStatus.Cancelled.name }

        assertTrue(previousOrderWithTrades.order.id != newOrderWithTrades.order.id)
        assertEquals(0, previousOrderWithTrades.trades.size)
        assertEquals(1, newOrderWithTrades.trades.size)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        val eventOrders = event.orders.filter { it.externalId == "1" }
        assertEquals(2, eventOrders.size)
        val eventPreviousOrder = eventOrders.first { it.status == OutgoingOrderStatus.CANCELLED }
        val eventNewOrder = eventOrders.first { it.status != OutgoingOrderStatus.CANCELLED }
        assertTrue { eventPreviousOrder.id != eventNewOrder.id }
        assertEquals(0, eventPreviousOrder.trades!!.size)
        assertEquals(1, eventNewOrder.trades!!.size)
    }

    @Test
    fun testRejectOrdersWithNotEnoughFunds() {
        setMultiOrderWithNotEnoughFunds(true)
        assertRejectOrdersWithNotEnoughFunds()
    }

    @Test
    fun testRejectNotSortedOrdersWithNotEnoughFunds() {
        setMultiOrderWithNotEnoughFunds(false)
        assertRejectOrdersWithNotEnoughFunds()
    }

    private fun setMultiOrderWithNotEnoughFunds(sorted: Boolean) {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 10.0)

        val order1 = IncomingLimitOrder(-300.0, 1.31)
        val order2 = IncomingLimitOrder(-300.0, 1.32)
        val order3 = IncomingLimitOrder(-300.0, 1.33)
        val order4 = IncomingLimitOrder(-300.0, 1.34)
        val order5 = IncomingLimitOrder(-300.0, 1.35)
        val order6 = IncomingLimitOrder(-100.0, 1.36)
        val order7 = IncomingLimitOrder(3.0, 1.2)
        val order8 = IncomingLimitOrder(3.0, 1.1)
        val order9 = IncomingLimitOrder(3.0, 1.0)
        val order10 = IncomingLimitOrder(3.0, 0.9)
        val order11 = IncomingLimitOrder(0.1, 0.8)

        val orders = if (sorted)
            listOf(order1,
                    order2,
                    order3,
                    order4,
                    order5,
                    order6,
                    order7,
                    order8,
                    order9,
                    order10,
                    order11)
        else
            listOf(order1,
                    order4,
                    order2,
                    order6,
                    order3,
                    order5,
                    order8,
                    order11,
                    order7,
                    order10,
                    order9)

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD",
                "Client1",
                orders))
    }

    private fun assertRejectOrdersWithNotEnoughFunds() {
        assertOrderBookSize("EURUSD", true, 4)
        assertOrderBookSize("EURUSD", false, 4)
        assertEquals(BigDecimal.valueOf(1.31), genericLimitOrderService.getOrderBook("EURUSD").getAskPrice())
        assertEquals(BigDecimal.valueOf(1.2), genericLimitOrderService.getOrderBook("EURUSD").getBidPrice())

        assertEquals(0, clientsEventsQueue.size)
        assertEquals(1, trustedClientsEventsQueue.size)
        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(8, event.orders.size)

        assertTrue(event.orders.any { it.price == "1.31" })
        assertTrue(event.orders.any { it.price == "1.32" })
        assertTrue(event.orders.any { it.price == "1.33" })
        assertTrue(event.orders.any { it.price == "1.36" })
        assertTrue(event.orders.any { it.price == "1.2" })
        assertTrue(event.orders.any { it.price == "1.1" })
        assertTrue(event.orders.any { it.price == "1" })
        assertTrue(event.orders.any { it.price == "0.8" })
    }

    @Test
    fun testRejectRoundingOrdersWithNotEnoughFunds() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 50.02)
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR",
                "Client1",
                listOf(IncomingLimitOrder(0.005, 5003.0, "1"),//25.015
                        IncomingLimitOrder(0.005, 5001.0, "2")))) //25.005

        assertOrderBookSize("BTCEUR", true, 1)
        assertEquals(BigDecimal.valueOf(5003), genericLimitOrderService.getOrderBook("BTCEUR").getBidPrice())

        assertEquals(0, clientsEventsQueue.size)
        assertEquals(1, trustedClientsEventsQueue.size)
        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals("1", event.orders.single().externalId)
    }

    @Test
    fun testMatchSellMinRemaining() {
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 50.00)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 0.02000199)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 49.99)

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", price = 5000.0, volume = 0.01, clientId = "Client2")))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(assetId = "BTCUSD", price = 4999.0, volume = 0.01, clientId = "Client3")))

        clearMessageQueues()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(
                IncomingLimitOrder(-0.01000199, 4998.0, "order1"),
                IncomingLimitOrder(-0.01, 4999.0, "order2")
        )))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(4, event.orders.size)

        val order1 = event.orders.single { it.externalId == "order1" }
        assertEquals(OutgoingOrderStatus.CANCELLED, order1.status)
        assertEquals(1, order1.trades?.size)
        assertEquals("BTC", order1.trades!![0].baseAssetId)
        assertEquals("-0.01", order1.trades!![0].baseVolume)
        assertEquals("USD", order1.trades!![0].quotingAssetId)
        assertEquals("50", order1.trades!![0].quotingVolume)
        assertEquals("Client2", order1.trades!![0].oppositeWalletId)
        assertEquals("-0.00000199", order1.remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.walletId == "Client2" }.status)

        val order2 = event.orders.single { it.externalId == "order2" }
        assertEquals(OutgoingOrderStatus.MATCHED, order2.status)
        assertEquals(1, order2.trades?.size)
        assertEquals("BTC", order1.trades!![0].baseAssetId)
        assertEquals("-0.01", order2.trades!![0].baseVolume)
        assertEquals("USD", order1.trades!![0].quotingAssetId)
        assertEquals("49.99", order2.trades!![0].quotingVolume)
        assertEquals("Client3", order2.trades!![0].oppositeWalletId)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.walletId == "Client3" }.status)

        assertOrderBookSize("BTCUSD", true, 0)
        assertOrderBookSize("BTCUSD", false, 0)

        assertBalance("Client2", "USD", reserved = 0.0)
        assertBalance("Client3", "USD", reserved = 0.0)
    }

    @Test
    fun testOrderMaxValue() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.1)
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8,
                maxValue = BigDecimal.valueOf(10000.0)))
        assetPairsCache.update()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(IncomingLimitOrder(-1.1, 10000.0))))

        assertOrderBookSize("BTCUSD", false, 0)
    }

    @Test
    fun testOrderMaxVolume() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.1)
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8,
                maxVolume = BigDecimal.valueOf(1.0)))
        assetPairsCache.update()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client1", listOf(IncomingLimitOrder(-1.1, 10000.0))))

        assertOrderBookSize("BTCUSD", false, 0)
    }

    @Test
    fun testImmediateOrCancelOrders() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 10.0))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(clientId = "Client1",
                pair = "EURUSD",
                orders = listOf(IncomingLimitOrder(6.0, 11.0, uid = "Matched", timeInForce = OrderTimeInForce.IOC),
                        IncomingLimitOrder(6.0, 10.0, uid = "PartiallyMatched", timeInForce = OrderTimeInForce.IOC),
                        IncomingLimitOrder(6.0, 9.0, uid = "WithoutTrades", timeInForce = OrderTimeInForce.IOC),
                        IncomingLimitOrder(6.0, 8.0, timeInForce = OrderTimeInForce.GTC))))

        assertOrderBookSize("EURUSD", false, 0)
        assertOrderBookSize("EURUSD", true, 1)
        assertEquals(BigDecimal.valueOf(8.0), genericLimitOrderService.getOrderBook("EURUSD").getBidPrice())

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.externalId == "Matched" }.status)

        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "PartiallyMatched" }.status)
        assertEquals(1, event.orders.single { it.externalId == "PartiallyMatched" }.trades?.size)

        assertEquals(1, trustedClientsEventsQueue.size)
        val trustedEvent = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.CANCELLED, trustedEvent.orders.single { it.externalId == "WithoutTrades" }.status)
        assertEquals(0, trustedEvent.orders.single { it.externalId == "WithoutTrades" }.trades?.size)

        assertBalance("Client1", "EUR", 1010.0, 0.0)
        assertBalance("Client1", "USD", 900.0, 0.0)
    }

    @Test
    fun testFillOrKillOrders() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 10.0))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(clientId = "Client1",
                pair = "EURUSD",
                orders = listOf(IncomingLimitOrder(6.0, 11.0, uid = "Matched", timeInForce = OrderTimeInForce.FOK),
                        IncomingLimitOrder(6.0, 10.0, uid = "PartiallyMatched", timeInForce = OrderTimeInForce.FOK),
                        IncomingLimitOrder(6.0, 9.0, uid = "WithoutTrades", timeInForce = OrderTimeInForce.FOK),
                        IncomingLimitOrder(6.0, 8.0, timeInForce = OrderTimeInForce.GTC))))

        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 1)
        assertEquals(BigDecimal.valueOf(8.0), genericLimitOrderService.getOrderBook("EURUSD").getBidPrice())

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.externalId == "Matched" }.status)

        assertEquals(1, trustedClientsEventsQueue.size)
        val trustedEvent = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.CANCELLED, trustedEvent.orders.single { it.externalId == "PartiallyMatched" }.status)
        assertEquals(0, trustedEvent.orders.single { it.externalId == "PartiallyMatched" }.trades?.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, trustedEvent.orders.single { it.externalId == "WithoutTrades" }.status)
        assertEquals(0, trustedEvent.orders.single { it.externalId == "WithoutTrades" }.trades?.size)

        assertBalance("Client1", "EUR", 1006.0, 0.0)
        assertBalance("Client1", "USD", 940.0, 0.0)
    }

}