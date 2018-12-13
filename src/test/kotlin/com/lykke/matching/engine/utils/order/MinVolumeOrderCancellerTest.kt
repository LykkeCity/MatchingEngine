package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
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
import kotlin.test.assertFalse
import kotlin.test.assertNull

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MinVolumeOrderCancellerTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MinVolumeOrderCancellerTest : AbstractTest() {

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @TestConfiguration
    open class Config {

        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))

            return testBackOfficeDatabaseAccessor
        }
        @Bean
        @Primary
        open fun testConfig(): TestSettingsDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
            testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("TrustedClient"))
            return testSettingsDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var recalculator: ReservedVolumesRecalculator

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "BTC", 2.0)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 3.0)
        testBalanceHolderWrapper.updateBalance("ClientForPartiallyMatching", "BTC", 3.0)

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 100.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "EUR", 200.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 300.0)
        testBalanceHolderWrapper.updateBalance("ClientForPartiallyMatching", "EUR", 300.0)

        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "USD", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 3000.0)
        testBalanceHolderWrapper.updateBalance("ClientForPartiallyMatching", "USD", 3000.0)

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCEUR", "BTC", "EUR", 5))

        initServices()
    }

    @Test
    fun testCancel() {
        // BTCEUR
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCEUR", price = 9000.0, volume = -1.0)))

        // BTCUSD
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 10000.0, volume = 0.00001)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "validVolume", clientId = "Client1", assetId = "BTCUSD", price = 10001.0, volume = 0.01)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 10001.0, volume = 0.001)))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(clientId = "TrustedClient", pair = "BTCUSD",
                orders = listOf(IncomingLimitOrder(0.00102, 10002.0), IncomingLimitOrder(-0.00001, 11000.0))))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "ClientForPartiallyMatching", assetId = "BTCUSD", price = 10002.0, volume = -0.001)))


        // EURUSD
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = -10.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.1, volume = 10.0)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client2", assetId = "EURUSD", price = 1.3, volume = -4.09)))
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "order2", clientId = "Client2", assetId = "EURUSD", price = 1.1, volume = 4.09)))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(clientId = "TrustedClient", pair = "EURUSD",
                orders = listOf(IncomingLimitOrder(30.0, 1.1), IncomingLimitOrder(-30.0, 1.4))))

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(clientId = "ClientForPartiallyMatching", assetId = "EURUSD", price = 1.2, volume = 6.0)))


        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 5, BigDecimal.valueOf(0.0001)))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 2,  BigDecimal.valueOf(5.0)))
        initServices()

        testTrustedClientsLimitOrderListener.clear()
        testClientLimitOrderListener.clear()
        balanceUpdateHandlerTest.clear()
        testRabbitOrderBookListener.clear()
        testOrderBookListener.clear()
        minVolumeOrderCanceller.cancel()

        assertEquals(BigDecimal.valueOf (2.001), testWalletDatabaseAccessor.getBalance("TrustedClient", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("TrustedClient", "BTC"))

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("TrustedClient", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("TrustedClient", "EUR"))

        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(BigDecimal.valueOf(1007.2), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(111.01), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(BigDecimal.valueOf(94.0), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))

        assertEquals(BigDecimal.valueOf(3000.0), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(10.01), testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))

        assertEquals(BigDecimal.valueOf(300.0), testWalletDatabaseAccessor.getBalance("Client2", "EUR"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        // BTCUSD
        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).filter { it.clientId == "Client1" }.size)
        // check order is removed from clientOrdersMap
        assertEquals(1, genericLimitOrderService.searchOrders("Client1", "BTCUSD", true).size)

        assertEquals("validVolume", testOrderDatabaseAccessor.getOrders("BTCUSD", true).first { it.clientId == "Client1" }.externalId)

        assertFalse(testOrderDatabaseAccessor.getOrders("BTCUSD", true).any { it.clientId == "TrustedClient" })
        assertFalse(testOrderDatabaseAccessor.getOrders("BTCUSD", false).any { it.clientId == "TrustedClient" })

        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).filter { it.clientId == "Client2" }.size)
        // check order is removed from clientOrdersMap
        assertEquals(1, genericLimitOrderService.searchOrders("Client2", "BTCUSD", true).size)

        // EURUSD
        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).filter { it.clientId == "Client1" }.size)
        assertFalse(testOrderDatabaseAccessor.getOrders("EURUSD", false).any { it.clientId == "Client1" })

        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).filter { it.clientId == "TrustedClient" }.size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", false).filter { it.clientId == "TrustedClient" }.size)

        assertFalse(testOrderDatabaseAccessor.getOrders("EURUSD", true).any { it.clientId == "Client2" })
        assertFalse(testOrderDatabaseAccessor.getOrders("EURUSD", false).any { it.clientId == "Client2" })

        // check order is removed from ordersMap
        assertNull(genericLimitOrderService.cancelLimitOrder(Date(), "order1", false))
        assertNull(genericLimitOrderService.cancelLimitOrder(Date(), "order2", false))

        assertEquals(1, testTrustedClientsLimitOrderListener.getCount())
        assertEquals(1, (testTrustedClientsLimitOrderListener.getQueue().first() as LimitOrdersReport).orders.size)
        assertEquals(BigDecimal.valueOf(11000.0), (testTrustedClientsLimitOrderListener.getQueue().first() as LimitOrdersReport).orders.first().order.price)

        assertEquals(1, testClientLimitOrderListener.getCount())
        assertEquals(5, (testClientLimitOrderListener.getQueue().first() as LimitOrdersReport).orders.size)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertEquals(4, testRabbitOrderBookListener.getCount())
        assertEquals(4, testOrderBookListener.getCount())
    }

    @Test
    fun testCancelOrdersWithRemovedAssetPair() {
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client1", assetId = "BTCEUR", price = 10000.0, volume = -1.0)))
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR", "TrustedClient",
                listOf(IncomingLimitOrder(-1.0, price = 10000.0, uid = "order2"))))

        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("TrustedClient", "BTC"))
        assertEquals(BigDecimal.valueOf( 1.0), balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(2, testOrderDatabaseAccessor.getOrders("BTCEUR", false).size)
        assertEquals(2, genericLimitOrderService.getOrderBook("BTCEUR").getOrderBook(false).size)

        testDictionariesDatabaseAccessor.clear() // remove asset pair BTCEUR
        initServices()
        minVolumeOrderCanceller.cancel()

        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCEUR", false).size)
        assertEquals(0, genericLimitOrderService.getOrderBook("BTCEUR").getOrderBook(false).size)

        // check order is removed from ordersMap
        assertNull(genericLimitOrderService.cancelLimitOrder(Date(), "order1", false))
        assertNull(genericLimitOrderService.cancelLimitOrder(Date(), "order2", false))

        // check order is removed from clientOrdersMap
        assertEquals(0, genericLimitOrderService.searchOrders("Client1", "BTCEUR", false).size)
        assertEquals(0, genericLimitOrderService.searchOrders("TrustedClient", "BTCEUR", false).size)

        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(1.0), balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("TrustedClient", "BTC"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedBalance("TrustedClient", "BTC"))

        // recalculate reserved volumes to reset locked reservedAmount
        recalculator.recalculate()
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
    }

}