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
import kotlin.test.assertTrue

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

        marketOrderPreprocessor.preProcess(messageBuilder.buildMarketOrderWrapper(MessageBuilder.buildMarketOrder(volume = 0.2, straight = false)))
        assertEquals(1, rabbitSwapListener.getCount())
        marketOrderReport = rabbitSwapListener.getQueue().poll() as MarketOrderWithTrades
        assertTrue(OrderStatus.TooSmallVolume.name != marketOrderReport.order.status)

        event = clientsEventsQueue.poll() as ExecutionEvent
        marketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertTrue(OrderRejectReason.TOO_SMALL_VOLUME != marketOrder.rejectReason)
    }
}