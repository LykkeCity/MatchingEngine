package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.holders.OrderBookMaxTotalSizeHolder
import com.lykke.matching.engine.holders.OrderBookMaxTotalSizeHolderImpl
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.socket.TestClientHandler
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
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
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (OrderBookMaxSizeTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrderBookMaxSizeTest : AbstractTest() {

    @TestConfiguration
    class Config {
        companion object {
            private const val MAX_SIZE = 3
        }

        @Bean
        @Primary
        fun orderBookMaxTotalSizeHolder(): OrderBookMaxTotalSizeHolder {
            return OrderBookMaxTotalSizeHolderImpl(MAX_SIZE)
        }
    }

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "BTC", balance = 1.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "EUR", balance = 2000.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 2000.0)
    }

    private fun setMaxSizeOrderBook() {
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 5000.0)
        ))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1,
                        type = LimitOrderType.STOP_LIMIT,
                        lowerLimitPrice = 1000.0, lowerPrice = 1000.0)
        ))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(clientId = "Client1", assetId = "EURUSD", volume = -100.0, price = 2.0)
        ))

        assertEquals(2, genericLimitOrderService.searchOrders("Client1", null, null).size)
        assertEquals(1, genericStopLimitOrderService.searchOrders("Client1", null, null).size)
        clearMessageQueues()
    }

    @Test
    fun testLimitOrder() {
        setMaxSizeOrderBook()

        val messageWrapper = messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 5000.0)
        )
        singleLimitOrderService.processMessage(messageWrapper)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(0, event.balanceUpdates?.size)
        assertEquals(1, event.orders.size)
        assertEquals(OrderStatus.REJECTED, event.orders.single().status)
        assertEquals(OrderRejectReason.ORDER_BOOK_MAX_SIZE_REACHED, event.orders.single().rejectReason)

        val clientHandler = messageWrapper.clientHandler!! as TestClientHandler
        assertEquals(1, clientHandler.responses.size)
        val response = clientHandler.responses.single()
        assertTrue(response is ProtocolMessages.NewResponse)
        response as ProtocolMessages.NewResponse
        assertEquals(MessageStatus.RUNTIME.type, response.status)
    }

    @Test
    fun testStopLimitOrder() {
        setMaxSizeOrderBook()

        val messageWrapper = messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1,
                        type = LimitOrderType.STOP_LIMIT,
                        lowerLimitPrice = 1000.0, lowerPrice = 1000.0)
        )
        singleLimitOrderService.processMessage(messageWrapper)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(0, event.balanceUpdates?.size)
        assertEquals(1, event.orders.size)
        assertEquals(OrderStatus.REJECTED, event.orders.single().status)
        assertEquals(OrderRejectReason.ORDER_BOOK_MAX_SIZE_REACHED, event.orders.single().rejectReason)

        val clientHandler = messageWrapper.clientHandler!! as TestClientHandler
        assertEquals(1, clientHandler.responses.size)
        val response = clientHandler.responses.single()
        assertTrue(response is ProtocolMessages.NewResponse)
        response as ProtocolMessages.NewResponse
        assertEquals(MessageStatus.RUNTIME.type, response.status)
    }

    @Test
    fun testMultiLimitOrder() {
        setMaxSizeOrderBook()

        val messageWrapper = messageBuilder.buildMultiLimitOrderWrapper(
                "EURUSD", "Client1", listOf(IncomingLimitOrder(-200.0, 3.0, "order1"),
                IncomingLimitOrder(-200.0, 3.1, uid = "order2")))
        multiLimitOrderService.processMessage(messageWrapper)


        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.balanceUpdates?.size)
        assertEquals(3, event.orders.size)
        assertNotEquals(OrderStatus.REJECTED, event.orders.single { it.externalId == "order1" }.status)
        assertEquals(OrderStatus.REJECTED, event.orders.single { it.externalId == "order2" }.status)
        assertEquals(OrderRejectReason.ORDER_BOOK_MAX_SIZE_REACHED, event.orders.single { it.externalId == "order2" }.rejectReason)

        val clientHandler = messageWrapper.clientHandler!! as TestClientHandler
        assertEquals(1, clientHandler.responses.size)
        val response = clientHandler.responses.single()
        assertTrue(response is ProtocolMessages.MultiLimitOrderResponse)
        response as ProtocolMessages.MultiLimitOrderResponse
        assertEquals(MessageStatus.OK.type, response.status)
        assertNotEquals(MessageStatus.RUNTIME.type, response.statusesList.single { it.id == "order1" }.status)
        assertEquals(MessageStatus.RUNTIME.type, response.statusesList.single { it.id == "order2" }.status)
    }

    @Test
    fun testMarketOrder() {
        setMaxSizeOrderBook()

        val messageWrapper = buildMarketOrderWrapper(
                buildMarketOrder(clientId = "Client1", assetId = "BTCUSD", volume = 0.1)
        )
        marketOrderService.processMessage(messageWrapper)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertNotEquals(OrderRejectReason.ORDER_BOOK_MAX_SIZE_REACHED, event.orders.single().rejectReason)

        val clientHandler = messageWrapper.clientHandler!! as TestClientHandler
        assertEquals(1, clientHandler.responses.size)
        val response = clientHandler.responses.single()
        assertTrue(response is ProtocolMessages.MarketOrderResponse)
        response as ProtocolMessages.MarketOrderResponse
        assertNotEquals(MessageStatus.RUNTIME.type, response.status)
    }
}