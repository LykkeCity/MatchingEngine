
package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
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
import java.util.Date
import java.util.UUID
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
        partiallyMatchedTrustedClientOrder.remainingVolume = -0.6
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
    }

    private fun buildMultiLimitOrderCancelWrapper(clientId: String, assetPairId: String, isBuy: Boolean): MessageWrapper {
        return MessageWrapper("Test", MessageType.MULTI_LIMIT_ORDER_CANCEL.type, ProtocolMessages.MultiLimitOrderCancel.newBuilder()
                .setUid(UUID.randomUUID().toString())
                .setTimestamp(Date().time)
                .setClientId(clientId)
                .setAssetPairId(assetPairId)
                .setIsBuy(isBuy).build().toByteArray(), null)
    }
}