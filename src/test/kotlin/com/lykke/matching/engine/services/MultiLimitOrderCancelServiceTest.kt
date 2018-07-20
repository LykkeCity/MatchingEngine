package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderCancelWrapper
import com.lykke.matching.engine.utils.assertEquals
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

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MultiLimitOrderCancelServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MultiLimitOrderCancelServiceTest : AbstractTest() {

    @Autowired
    private lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

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

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testConfigDatabaseAccessor.addTrustedClient("TrustedClient")

        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC",  1.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "BTC", 1.0)

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.4, price = 10000.0, reservedVolume = 0.4))
        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.6, price = 11000.0, reservedVolume = 0.6))

        testOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "TrustedClient", assetId = "BTCUSD", volume = -0.3, price = 10500.0))
        val partiallyMatchedTrustedClientOrder = buildLimitOrder(clientId = "TrustedClient", assetId = "BTCUSD", volume = -0.7, price = 11500.0)
        partiallyMatchedTrustedClientOrder.remainingVolume = BigDecimal.valueOf(-0.6)
        testOrderDatabaseAccessor.addLimitOrder(partiallyMatchedTrustedClientOrder)

        initServices()
    }

    @Test
    fun testCancelTrustedClientOrders() {
        val messageWrapper = buildMultiLimitOrderCancelWrapper("TrustedClient", "BTCUSD", false)
        multiLimitOrderCancelService.parseMessage(messageWrapper)
        multiLimitOrderCancelService.processMessage(messageWrapper)

        assertOrderBookSize("BTCUSD", false, 2)
        assertBalance("TrustedClient", "BTC", 1.0, 0.0)
        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(1, (clientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)
        assertEquals(1, trustedClientsLimitOrdersQueue.size)
        assertEquals(1, (trustedClientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)

        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, tradesInfoQueue.size)
        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(1, (clientsEventsQueue.first() as ExecutionEvent).orders.size)
        assertEquals(0, (clientsEventsQueue.first() as ExecutionEvent).balanceUpdates!!.size)
        assertEquals(1, trustedClientsEventsQueue.size)
        assertEquals(1, (trustedClientsEventsQueue.first() as ExecutionEvent).orders.size)
        assertEquals(0, (trustedClientsEventsQueue.first() as ExecutionEvent).balanceUpdates!!.size)

        assertEquals(1, tradesInfoQueue.size)
        val tradeInfo = tradesInfoQueue.poll() as TradeInfo
        assertEquals(BigDecimal.valueOf(10000.0), tradeInfo.price)
        assertEquals(false, tradeInfo.isBuy)
    }

    @Test
    fun testCancelClientOrders() {
        val messageWrapper = buildMultiLimitOrderCancelWrapper("Client1", "BTCUSD", false)
        multiLimitOrderCancelService.parseMessage(messageWrapper)
        multiLimitOrderCancelService.processMessage(messageWrapper)

        assertOrderBookSize("BTCUSD", false, 2)
        assertBalance("Client1", "BTC", 1.0, 0.0)
        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(2, (clientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)
        assertEquals(0, trustedClientsLimitOrdersQueue.size)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, tradesInfoQueue.size)
        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)

        assertEquals(0, trustedClientsEventsQueue.size)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.balanceUpdates?.size)
        event.orders.forEach {
            assertEquals(OutgoingOrderStatus.CANCELLED, it.status)
        }

    }
}