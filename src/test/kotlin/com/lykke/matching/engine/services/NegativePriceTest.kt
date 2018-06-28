package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
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
import java.math.BigDecimal
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (NegativePriceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class NegativePriceTest : AbstractTest() {

    @Autowired
    private lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

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

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client", "USD", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client", "USD", 0.0)

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))

        initServices()
    }

    @Test
    fun testLimitOrder() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client", assetId = "EURUSD", price = -1.0, volume = 1.0)))

        assertEquals(1, testClientLimitOrderListener.getCount())
        val result = testClientLimitOrderListener.getQueue().poll() as LimitOrdersReport

        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InvalidPrice.name, result.orders.first().order.status)
    }

    @Test
    fun testTrustedClientMultiLimitOrder() {
        testConfigDatabaseAccessor.addTrustedClient("Client")

        initServices()

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD",
                "Client",
                listOf(
                        VolumePrice(BigDecimal.valueOf(1.0), BigDecimal.valueOf(1.0)),
                        VolumePrice(BigDecimal.valueOf(1.0), BigDecimal.valueOf(-1.0))
                ),
                emptyList(),
                emptyList(),
                listOf("order1", "order2")))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        val result = testTrustedClientsLimitOrderListener.getQueue().poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InOrderBook.name, result.orders.first { it.order.externalId == "order1" }.order.status)

        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).size)
    }
}