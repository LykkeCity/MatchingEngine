package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.MidPriceHolder
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderCancelWrapper
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.getExecutionContext
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
import java.util.*
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MultiLimitOrderCancelServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MultiLimitOrderCancelServiceTest : AbstractTest() {
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

    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    @Autowired
    private lateinit var midPriceHolder: MidPriceHolder

    @Autowired
    private lateinit var assetPairHolder: AssetsPairsHolder

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8, midPriceDeviationThreshold = BigDecimal.valueOf(0.1)))

        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("TrustedClient"))

        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "BTC", 1.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.4, price = 10000.0, reservedVolume = 0.4))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", volume = -0.6, price = 11000.0, reservedVolume = 0.6))

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(clientId = "TrustedClient", assetId = "BTCUSD", volume = -0.3, price = 10500.0))
        val partiallyMatchedTrustedClientOrder = buildLimitOrder(clientId = "TrustedClient", assetId = "BTCUSD", volume = -0.7, price = 11500.0)
        partiallyMatchedTrustedClientOrder.remainingVolume = BigDecimal.valueOf(-0.6)
        testOrderBookWrapper.addLimitOrder(partiallyMatchedTrustedClientOrder)

        initServices()
    }

    @Test
    fun testCancelTrustedClientOrders() {
        val messageWrapper = buildMultiLimitOrderCancelWrapper("TrustedClient", "BTCUSD", false)
        multiLimitOrderCancelService.parseMessage(messageWrapper)
        multiLimitOrderCancelService.processMessage(messageWrapper)

        assertOrderBookSize("BTCUSD", false, 2)
        assertBalance("TrustedClient", "BTC", 1.0, 0.0)
        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(1, (testClientLimitOrderListener.getQueue().first() as LimitOrdersReport).orders.size)
        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        assertEquals(1, (testTrustedClientsLimitOrderListener.getQueue().first() as LimitOrdersReport).orders.size)

        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, tradesInfoListener.getCount())
        assertEquals(1, testOrderBookListener.getCount())
        assertEquals(1, testRabbitOrderBookListener.getCount())

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(1, (clientsEventsQueue.first() as ExecutionEvent).orders.size)
        assertEquals(0, (clientsEventsQueue.first() as ExecutionEvent).balanceUpdates!!.size)
        assertEquals(1, trustedClientsEventsQueue.size)
        assertEquals(1, (trustedClientsEventsQueue.first() as ExecutionEvent).orders.size)
        assertEquals(0, (trustedClientsEventsQueue.first() as ExecutionEvent).balanceUpdates!!.size)

        assertEquals(1, tradesInfoListener.getCount())
        val tradeInfo = tradesInfoListener.getProcessingQueue().poll()
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
        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(2, (testClientLimitOrderListener.getQueue().first() as LimitOrdersReport).orders.size)
        assertEquals(0, testTrustedClientsLimitOrderListener.getCount())

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        assertEquals(1, tradesInfoListener.getCount())
        assertEquals(1, testOrderBookListener.getCount())
        assertEquals(1, testRabbitOrderBookListener.getCount())

        assertEquals(0, trustedClientsEventsQueue.size)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.balanceUpdates?.size)
        event.orders.forEach {
            assertEquals(OutgoingOrderStatus.CANCELLED, it.status)
        }
    }

    @Test
    fun midPriceIsRecordedMultilimitOrderCancel() {
        //given
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.01, price = 9000.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", volume = 0.01, price = 8900.0)))
        assertEquals(BigDecimal.valueOf(9500.0), midPriceHolder.getReferenceMidPrice(assetPairHolder.getAssetPair("BTCUSD"), getExecutionContext(Date(), executionContextFactory)))

        //when
        val messageWrapper = buildMultiLimitOrderCancelWrapper("Client1", "BTCUSD", false)
        multiLimitOrderCancelService.parseMessage(messageWrapper)
        multiLimitOrderCancelService.processMessage(messageWrapper)
        //then
        assertEquals(BigDecimal.valueOf(9583.33333334), midPriceHolder.getReferenceMidPrice(assetPairHolder.getAssetPair("BTCUSD"), getExecutionContext(Date(), executionContextFactory)))
    }
}