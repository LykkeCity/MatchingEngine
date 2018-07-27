package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import kotlin.test.assertEquals
import com.lykke.matching.engine.utils.assertEquals
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.assertFalse
import kotlin.test.assertNull

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (LimitOrderCancelServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LimitOrderCancelServiceTest : AbstractTest() {

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Before
    fun setUp() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "5", price = 100.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "3", price = 300.0, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "6", price = 200.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "7", price = 300.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "8", price = 400.0))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURCHF", "EUR", "CHF", 5))

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR",  1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        initServices()
    }

    @Test
    fun testCancel() {
        limitOrderCancelService.processMessage(MessageBuilder.buildLimitOrderCancelWrapper("3"))

        assertEquals(1, testOrderBookListener.getCount())
        assertEquals(1, testRabbitOrderBookListener.getCount())

        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(OrderStatus.Cancelled.name, (testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport).orders.first().order.status)
        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdateNotifications())
        assertEquals("Client1", balanceUpdateHandlerTest.balanceUpdateNotificationQueue.poll().clientId)
        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(1, balanceUpdate.balances.size)
        assertEquals("Client1", balanceUpdate.balances.first().id)
        assertEquals("EUR", balanceUpdate.balances.first().asset)
        assertEquals(BigDecimal.valueOf(1000.0), balanceUpdate.balances.first().oldBalance)
        assertEquals(BigDecimal.valueOf(1000.0), balanceUpdate.balances.first().newBalance)
        assertEquals(BigDecimal.valueOf(1.0), balanceUpdate.balances.first().oldReserved)
        assertEquals(BigDecimal.ZERO, balanceUpdate.balances.first().newReserved)

        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("Client1", "EUR"))

        val order = testOrderDatabaseAccessor.loadLimitOrders().find { it.id == "3" }
        assertNull(order)
        assertEquals(4, testOrderDatabaseAccessor.loadLimitOrders().size)

        val previousOrders = genericLimitOrderService.searchOrders("Client1", "EURUSD", true)
        assertEquals(4, previousOrders.size)
        assertFalse(previousOrders.any { it.externalId == "3" })

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, executionEvent.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "EUR", "1000", "1000", "1", "0", executionEvent.balanceUpdates!!)
        assertEquals(1, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders.first().status)

        assertEquals(1, tradesInfoListener.getCount())
        val tradeInfo = tradesInfoListener.getProcessingQueue().poll()
        assertEquals(BigDecimal.ZERO, tradeInfo.price)
        assertEquals(false, tradeInfo.isBuy)
    }

    @Test
    fun testMultiCancel() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 5))
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 1.0)
        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "10", clientId = "Client2", assetId = "BTCUSD", price = 9000.0, volume = -0.5)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "11", clientId = "Client2", assetId = "BTCUSD", price = 9100.0, volume = -0.3)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "12", clientId = "Client2", assetId = "BTCUSD", price = 9200.0, volume = -0.2)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "13", clientId = "Client2", assetId = "BTCUSD", price = 8000.0, volume = 0.1)))
        clearMessageQueues()

        limitOrderCancelService.processMessage(MessageBuilder.buildLimitOrderCancelWrapper(listOf("10", "11", "13")))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 0)

        assertBalance("Client2", "BTC", 1.0, 0.2)
        assertBalance("Client2", "USD", 1000.0, 0.0)

        assertEquals(2, testOrderBookListener.getCount())
        assertEquals(2, testRabbitOrderBookListener.getCount())
        assertEquals(1, testClientLimitOrderListener.getCount())

        val report = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(3, report.orders.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "10" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "11" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "13" }.order.status)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(MessageType.LIMIT_ORDER_CANCEL.name, balanceUpdate.type)
        assertEquals(2, balanceUpdate.balances.size)

        val btc = balanceUpdate.balances.first { it.asset == "BTC" }
        assertEquals("Client2", btc.id)
        assertEquals(BigDecimal.valueOf(1.0), btc.oldReserved)
        assertEquals(BigDecimal.valueOf(0.2), btc.newReserved)

        val usd = balanceUpdate.balances.first { it.asset == "USD" }
        assertEquals("Client2", usd.id)
        assertEquals(BigDecimal.valueOf(800.0), usd.oldReserved)
        assertEquals(BigDecimal.ZERO, usd.newReserved)

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, executionEvent.balanceUpdates?.size)

        assertEventBalanceUpdate("Client2", "BTC", "1", "1", "1", "0.2", executionEvent.balanceUpdates!!)
        assertEventBalanceUpdate("Client2", "USD", "1000", "1000", "800", "0", executionEvent.balanceUpdates!!)

        assertEquals(3, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders[0].status)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders[1].status)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders[2].status)

        assertEquals(2, tradesInfoListener.getCount())
        val buyTradeInfo = tradesInfoListener.getProcessingQueue().single { it.isBuy }
        assertEquals(BigDecimal.ZERO, buyTradeInfo.price)
        val sellTradeInfo = tradesInfoListener.getProcessingQueue().single { !it.isBuy }
        assertEquals(BigDecimal.valueOf(9200.0), sellTradeInfo.price)
    }
}