
package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.database.BackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderMassCancelWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
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


@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (LimitOrderMassCancelServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LimitOrderMassCancelServiceTest : AbstractTest() {

    
    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))

            return testBackOfficeDatabaseAccessor
        }

        @Bean
        @Primary
        open fun testConfig(): TestConfigDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestConfigDatabaseAccessor()
            testSettingsDatabaseAccessor.addTrustedClient("TrustedClient")
            return testSettingsDatabaseAccessor
        }
    }
    
    @Before
    fun setUp() {

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 2))

        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 100.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "EUR", 10.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "USD", 10.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "BTC", 1.0)

        initServices()
    }

    private fun setOrders() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "1", clientId = "Client1", assetId = "BTCUSD", volume = -0.5, price = 9000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "2", clientId = "Client1", assetId = "BTCUSD", volume = -0.1, price = 9000.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "3", clientId = "Client1", assetId = "BTCUSD", volume = 0.01, price = 7000.0)))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "4", clientId = "Client1", assetId = "EURUSD", volume = 10.0, price = 1.1)))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("EURUSD", "TrustedClient", listOf(
                VolumePrice(-5.0, 1.3),
                VolumePrice(5.0, 1.1)
        ), emptyList(), emptyList(), listOf("m1", "m2")))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCUSD", "TrustedClient", listOf(
                VolumePrice(-1.0, 8500.0)
        ), emptyList(), emptyList()))

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 1)
        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 2)
        clearMessageQueues()
    }

    @Test
    fun testCancelOrdersOneSide() {
        setOrders()

        limitOrderMassCancelService.processMessage(buildLimitOrderMassCancelWrapper("Client1", "BTCUSD", false))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 1)
        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 2)

        assertBalance("Client1", "BTC", 1.0, 0.0)
        assertEquals(1, orderBookQueue.size)
        assertEquals(1, rabbitOrderBookQueue.size)
        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "1" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "2" }.order.status)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(MessageType.LIMIT_ORDER_MASS_CANCEL.name, balanceUpdate.type)
        assertEquals(1, balanceUpdate.balances.size)
        assertEquals("Client1", balanceUpdate.balances.first().id)
        assertEquals(BigDecimal.valueOf(0.6), balanceUpdate.balances.first().oldReserved)
        assertEquals(BigDecimal.ZERO, balanceUpdate.balances.first().newReserved)
    }

    @Test
    fun cancelAllClientOrders() {
        setOrders()

        limitOrderMassCancelService.processMessage(buildLimitOrderMassCancelWrapper("Client1"))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 0)
        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 1)

        assertBalance("Client1", "BTC", 1.0, 0.0)
        assertBalance("Client1", "USD", 100.0, 0.0)
        assertEquals(3, orderBookQueue.size)
        assertEquals(3, rabbitOrderBookQueue.size)
        assertEquals(1, clientsLimitOrdersQueue.size)

        val report = clientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(4, report.orders.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "1" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "2" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "3" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "4" }.order.status)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
        assertEquals(MessageType.LIMIT_ORDER_MASS_CANCEL.name, balanceUpdate.type)
        assertEquals(2, balanceUpdate.balances.size)

        val btc = balanceUpdate.balances.first { it.asset == "BTC" }
        assertEquals("Client1", btc.id)
        assertEquals(BigDecimal.valueOf(0.6), btc.oldReserved)
        assertEquals(BigDecimal.ZERO, btc.newReserved)

        val usd = balanceUpdate.balances.first { it.asset == "USD" }
        assertEquals("Client1", usd.id)
        assertEquals(BigDecimal.valueOf(81.0), usd.oldReserved)
        assertEquals(BigDecimal.ZERO, usd.newReserved)
    }

    @Test
    fun testCancelTrustedClientOrders() {
        setOrders()

        limitOrderMassCancelService.processMessage(buildLimitOrderMassCancelWrapper("TrustedClient", "EURUSD"))

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 1)
        assertOrderBookSize("EURUSD", false, 0)
        assertOrderBookSize("EURUSD", true, 1)

        assertEquals(2, orderBookQueue.size)
        assertEquals(2, rabbitOrderBookQueue.size)
        assertEquals(0, clientsLimitOrdersQueue.size)
        assertEquals(1, trustedClientsLimitOrdersQueue.size)

        val report = trustedClientsLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(2, report.orders.size)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "m1" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, report.orders.first { it.order.externalId == "m2" }.order.status)

        assertEquals(0, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
    }

}