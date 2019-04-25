package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderType
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MarketOrderPreprocessorTest: AbstractTest() {

    @Autowired
    private lateinit var marketOrderPreprocessor: MarketOrderPreprocessor

    @Test
    fun testValidateMessageIsPushedFromPreprocessorDataInvalid() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)))
        initServices()

        marketOrderPreprocessor.preProcess(messageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(volume = 0.09)))
        assertEquals(1, rabbitSwapListener.getCount())
        var marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.TooSmallVolume.name, marketOrderReport.order.status)

        var event = clientsEventsQueue.poll() as ExecutionEvent
        var marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus.REJECTED, marketOrder.status)
        assertEquals(OrderRejectReason.TOO_SMALL_VOLUME, marketOrder.rejectReason)

        //straight = false
        marketOrderPreprocessor.preProcess(messageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(volume = -0.19, straight = false)))
        assertEquals(1, rabbitSwapListener.getCount())
        marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.TooSmallVolume.name, marketOrderReport.order.status)

        event = clientsEventsQueue.poll() as ExecutionEvent
        marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus.REJECTED, marketOrder.status)
        assertEquals(OrderRejectReason.TOO_SMALL_VOLUME, marketOrder.rejectReason)
    }

    @Test
    fun testNotStraightOrderMaxValue() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8,
                maxValue = BigDecimal.valueOf(10000)))
        assetPairsCache.update()

        marketOrderPreprocessor.preProcess(messageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client1", assetId = "BTCUSD", volume = 10001.0, straight = false)))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventOrder = event.orders.single()
        assertEquals(com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.INVALID_VALUE, eventOrder.rejectReason)
    }

    @Test
    fun testStraightOrderMaxVolume() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.1)
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8,
                maxVolume = BigDecimal.valueOf(1.0)))
        assetPairsCache.update()

        marketOrderPreprocessor.preProcess(messageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(clientId = "Client1", assetId = "BTCUSD", volume = -1.1)))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventOrder = event.orders.single()
        assertEquals(com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.INVALID_VOLUME, eventOrder.rejectReason)
    }
}