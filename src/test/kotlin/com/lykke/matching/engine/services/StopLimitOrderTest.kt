package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderCancelWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderCancelWrapper
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
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (StopLimitOrderTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class StopLimitOrderTest : AbstractTest() {

    @Autowired
    private lateinit var balanceUpdateHandlerTest: BalanceUpdateHandlerTest

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

        @Bean
        @Primary
        open fun testWalletDatabaseAccessor(): TestWalletDatabaseAccessor {
            val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()

            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", 1.0, 0.0))
            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0, 0.0))

            return testWalletDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 6))
        initServices()
    }

    @Test
    fun testNotEnoughFunds() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -1.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))

        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(OrderStatus.NotEnoughFunds.name, report.orders.first().order.status)

        assertEquals(0, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
    }

    @Test
    fun testInvalidPrice() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -1.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0,
                upperLimitPrice = 9500.0, upperPrice = 9100.0
        )))

        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(OrderStatus.InvalidPrice.name, report.orders.first().order.status)

        assertEquals(0, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
    }

    @Test
    fun testAddStopLimitOrder() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))

        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(OrderStatus.Pending.name, report.orders.first().order.status)
        assertEquals(0.01, report.orders.first().order.reservedLimitVolume)
        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(0.01, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(0.01, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
    }

    @Test
    fun testAddStopLimitOrderAndCancelAllPrevious() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))
        balanceUpdateHandlerTest.clear()
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = -0.02,
                lowerLimitPrice = 9500.0, lowerPrice = 9000.0, upperLimitPrice = 10500.0, upperPrice = 10000.0

        ), true))

        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.status == OrderStatus.Cancelled.name }.size)
        assertEquals(1, report.orders.filter { it.order.status == OrderStatus.Pending.name }.size)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(-0.02, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).first().volume)
        assertEquals(0.02, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(0.02, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
    }

    @Test
    fun testCancelStopLimitOrder() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))

        clientsLimitOrdersQueue.clear()
        limitOrderCancelService.processMessage(buildLimitOrderCancelWrapper("order1"))

        assertTrue(stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).isEmpty())
        assertTrue(genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first().order.status)
    }

    @Test
    fun testProcessStopLimitOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9000.0
        )))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order2", clientId = "Client1", assetId = "BTCUSD", volume = -0.03,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.5, lowerPrice = 9000.0
        )))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.03, price = 10000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.03, price = 9501.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.03, price = 9499.0)))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.03, price = 10000.0)))
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.03, price = 9501.0)))

        assertEquals(0.01, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(0.01, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(2, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.last() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order2" }.size)
        val stopOrder = report.orders.first { it.order.externalId == "order2" }

        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(9000.0, stopOrder.order.price)
    }

    @Test
    fun testProcessStopOrderAfterRejectLimitOrderWithCancelPrevious() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.1))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 0.1))

        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 10000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.1, price = 11000.0)))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = 0.05, type = LimitOrderType.STOP_LIMIT,
                upperLimitPrice = 10500.0, upperPrice = 11000.0
        )))

        clientsLimitOrdersQueue.clear()
        // cancel previous orders and will be rejected due to not enough funds
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2, price = 10000.0), true))

        assertEquals(2, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.last() as LimitOrdersReport

        assertEquals(2, report.orders.size)
        val clientOrders = report.orders.filter { it.order.clientId == "Client1" }
        assertEquals(1, clientOrders.size)
        val clientOrder = clientOrders.first()
        assertEquals(1, clientOrder.trades.size)
        assertEquals("550.00", clientOrder.trades.first().volume)
        assertEquals("0.05000000", clientOrder.trades.first().oppositeVolume)

        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
        assertEquals(0, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).size)

        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testProcessStopLimitOrderAfterLimitOrderCancellation() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.3))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 10000.0, upperPrice = 10500.0
        )))
        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(945.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(945.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", uid = "order2", assetId = "BTCUSD", volume = -0.1, price = 9000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2, price = 10000.0)))

        clientsLimitOrdersQueue.clear()
        limitOrderCancelService.processMessage(buildLimitOrderCancelWrapper("order2"))

        assertEquals(0, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(100.0, balancesHolder.getBalance("Client1", "USD"))
        assertEquals(100.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(2, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.last() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(10500.0, stopOrder.order.price)
    }

    @Test
    fun testRejectStopOrderDuringMatching() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.03, price = 9600.0)))

        // Order leading to negative spread. Added to reject stop order.
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.03, price = 9500.0)))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.01,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.5, lowerPrice = 9000.0
        )))

        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.03, price = 9600.0)))

        assertEquals(0, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", false).size)
        assertEquals(1, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).size)

        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(2, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.last() as LimitOrdersReport
        assertEquals(1, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.status == OrderStatus.LeadToNegativeSpread.name }.size)
    }

    @Test
    fun testProcessStopLimitOrderAfterMarketOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.3))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1900.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 10000.0, upperPrice = 10500.0
        )))
        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(945.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(945.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 9000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2, price = 10000.0)))

        clientsLimitOrdersQueue.clear()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(clientId = "Client3", assetId = "BTCUSD", volume = 0.2)))

        assertEquals(0, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(100.0, balancesHolder.getBalance("Client1", "USD"))
        assertEquals(100.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(2, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.last() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(10500.0, stopOrder.order.price)
    }

    @Test
    fun testProcessStopLimitOrderAfterMultiLimitOrder() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.3))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 1.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 8500.0, lowerPrice = 9000.0, upperLimitPrice = 10000.0, upperPrice = 10500.0
        )))
        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(945.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(945.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 9000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.2, price = 10000.0)))

        clientsLimitOrdersQueue.clear()
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client3",
                listOf(VolumePrice(-0.1, 11000.0), VolumePrice(-0.05, 8500.0)), emptyList(), emptyList()))

        assertEquals(0, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(215.0, balancesHolder.getBalance("Client1", "USD"))
        assertEquals(215.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.last() as LimitOrdersReport
        assertEquals(3, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(9000.0, stopOrder.order.price)
    }

    @Test
    fun testProcessStopLimitOrderAfterMultiLimitOrderCancellation() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.3))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", 0.1))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 10000.0, upperPrice = 10500.0
        )))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client2",
                listOf(VolumePrice(-0.1, 9000.0), VolumePrice(-0.2, 10000.0)), emptyList(), emptyList()))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client3", assetId = "BTCUSD", volume = -0.1, price = 10000.0)))

        clientsLimitOrdersQueue.clear()
        val message = buildMultiLimitOrderCancelWrapper("Client2", "BTCUSD", false)
        multiLimitOrderCancelService.parseMessage(message)
        multiLimitOrderCancelService.processMessage(message)

        assertEquals(0, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(0, stopOrderDatabaseAccessor.getStopOrders("BTCUSD", true).size)
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(100.0, balancesHolder.getBalance("Client1", "USD"))
        assertEquals(100.0, testWalletDatabaseAccessor.getBalance("Client1", "USD"))

        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(10500.0, stopOrder.order.price)
    }

    @Test
    fun testProcessStopLimitOrderAfterMinVolumeOrdersCancellation() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", 5.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 10000.0))

        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 2))
        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client2", listOf(
                VolumePrice(-0.00009, 10000.0), VolumePrice(-0.09, 11000.0)
        ), emptyList(), emptyList()))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD", "Client2", listOf(
                VolumePrice(1.0, 1.1), VolumePrice(6.0, 1.0)
        ), emptyList(), emptyList()))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = 0.09,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 11000.0, upperPrice = 11000.0
        )))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "EURUSD", volume = -5.0,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 1.0, lowerPrice = 0.99
        )))

        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(2, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("EURUSD").getOrderBook(false).size)
        assertEquals(2, genericLimitOrderService.getOrderBook("EURUSD").getOrderBook(true).size)
        assertEquals(990.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(5.0, balancesHolder.getReservedBalance("Client1", "EUR"))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 5, 0.0001))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 2, 5.0))

        initServices()

        clientsLimitOrdersQueue.clear()
        minVolumeOrderCanceller.cancel()

        assertEquals(3, clientsLimitOrdersQueue.size)
        assertTrue(genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericStopLimitOrderService.getStopOrderBook("EURUSD").getOrderBook(false).isEmpty())
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "EUR"))
        assertEquals(15.0, balancesHolder.getBalance("Client1", "USD"))
        assertEquals(0.0, balancesHolder.getBalance("Client1", "EUR"))
    }

    @Test
    fun testProcessStopLimitOrdersChain() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 10000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 10000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 1.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1, price = 10000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1, price = 9500.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1, price = 9000.0)))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.2,
                type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 10500.0, upperPrice = 10000.0
        )))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9500.0, lowerPrice = 9500.0
        )))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.2,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9000.0, lowerPrice = 9000.0
        )))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client3", assetId = "BTCUSD", volume = 0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9000.0, lowerPrice = 9000.0
        )))

        assertEquals(3, genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(3, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(0.5, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(900.0, balancesHolder.getReservedBalance("Client3", "USD"))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.1, price = 10500.0)))


        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertTrue(genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(0.0, balancesHolder.getReservedBalance("Client3", "USD"))
    }

    @Test
    fun testProcessBothSideStopLimitOrders() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1050.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.1))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 850.0))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9000.0, lowerPrice = 8500.0
        )))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client3", assetId = "BTCUSD", volume = 0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 10000.0, lowerPrice = 10500.0
        )))

        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).size)
        assertEquals(1, genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).size)
        assertEquals(0.1, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(1050.0, balancesHolder.getReservedBalance("Client3", "USD"))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "Client2", listOf(
                VolumePrice(-0.1, 10000.0), VolumePrice(0.1, 9000.0)
        ), emptyList(), emptyList()))

        assertTrue(genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(0.0, balancesHolder.getReservedBalance("Client3", "USD"))
    }

    @Test
    fun testProcessStopLimitOrderImmediately() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", 0.1))
        initServices()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = -0.1, price = 9900.0)))
        clientsLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client1", assetId = "BTCUSD", volume = 0.1,
                type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 9900.0, lowerPrice = 10000.0)))

        assertTrue(genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericStopLimitOrderService.getStopOrderBook("BTCUSD").getOrderBook(false).isEmpty())
        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(true).isEmpty())
        assertTrue(genericLimitOrderService.getOrderBook("BTCUSD").getOrderBook(false).isEmpty())

        assertEquals(0.0, balancesHolder.getReservedBalance("Client1", "USD"))
        assertEquals(10.0, balancesHolder.getBalance("Client1", "USD"))

        assertEquals(1, clientsLimitOrdersQueue.size)
        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(1, report.orders.filter { it.order.externalId == "order1" }.size)

        val stopOrder = report.orders.first { it.order.externalId == "order1" }
        assertEquals(OrderStatus.Matched.name, stopOrder.order.status)
        assertEquals(10000.0, stopOrder.order.price)
    }

}