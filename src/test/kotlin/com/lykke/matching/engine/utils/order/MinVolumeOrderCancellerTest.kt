package com.lykke.matching.engine.utils.order

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.VolumePrice
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.database.TestReservedVolumesDatabaseAccessor
import com.lykke.matching.engine.database.TestStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MinVolumeOrderCancellerTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MinVolumeOrderCancellerTest : AbstractTest() {

    private lateinit var canceller: MinVolumeOrderCanceller

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
        open fun testConfig(): TestConfigDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestConfigDatabaseAccessor()
            testSettingsDatabaseAccessor.addTrustedClient("TrustedClient")
            return testSettingsDatabaseAccessor
        }
    }

    @Autowired
    lateinit var applicationContext: ApplicationContext

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

    override fun initServices() {
        super.initServices()
        canceller = MinVolumeOrderCanceller(testDictionariesDatabaseAccessor, assetsPairsHolder, genericLimitOrderService, genericLimitOrdersCancellerFactory)
    }

    @Test
    fun testCancel() {
        // BTCEUR
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCEUR", price = 9000.0, volume = -1.0)))

        // BTCUSD
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 10000.0, volume = 0.00001)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "validVolume", clientId = "Client1", assetId = "BTCUSD", price = 10001.0, volume = 0.01)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 10001.0, volume = 0.001)))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(clientId = "TrustedClient", pair = "BTCUSD",
                volumes = listOf(VolumePrice(0.00102, 10002.0), VolumePrice(-0.00001, 11000.0)),
                ordersFee = emptyList(), ordersFees = emptyList()))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "ClientForPartiallyMatching", assetId = "BTCUSD", price = 10002.0, volume = -0.001)))


        // EURUSD
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = -10.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.1, volume = 10.0)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client2", assetId = "EURUSD", price = 1.3, volume = -4.09)))
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "order2", clientId = "Client2", assetId = "EURUSD", price = 1.1, volume = 4.09)))

        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper(clientId = "TrustedClient", pair = "EURUSD",
                volumes = listOf(VolumePrice(30.0, 1.1), VolumePrice(-30.0, 1.4)),
                ordersFee = emptyList(), ordersFees = emptyList()))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(clientId = "ClientForPartiallyMatching", assetId = "EURUSD", price = 1.2, volume = 6.0)))


        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 5, 0.0001))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 2, 5.0))
        initServices()

        trustedClientsLimitOrdersQueue.clear()
        clientsLimitOrdersQueue.clear()
        balanceUpdateHandlerTest.clear()
        rabbitOrderBookQueue.clear()
        orderBookQueue.clear()
        canceller.cancel()

        assertEquals(2.001, testWalletDatabaseAccessor.getBalance("TrustedClient", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("TrustedClient", "BTC"))

        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("TrustedClient", "USD"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("TrustedClient", "EUR"))

        assertEquals(1.0, testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(1.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(1007.2, testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(111.01, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(94.0, testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))

        assertEquals(3000.0, testWalletDatabaseAccessor.getBalance("Client2", "USD"))
        assertEquals(10.01, testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))

        assertEquals(300.0, testWalletDatabaseAccessor.getBalance("Client2", "EUR"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        // BTCUSD
        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).filter { it.clientId == "Client1" }.size)
        // check order is removed from clientOrdersMap
        assertEquals(1, genericLimitOrderService.getAllPreviousOrders("Client1", "BTCUSD", true).size)

        assertEquals("validVolume", testOrderDatabaseAccessor.getOrders("BTCUSD", true).first { it.clientId == "Client1" }.externalId)

        assertFalse(testOrderDatabaseAccessor.getOrders("BTCUSD", true).any { it.clientId == "TrustedClient" })
        assertFalse(testOrderDatabaseAccessor.getOrders("BTCUSD", false).any { it.clientId == "TrustedClient" })

        assertEquals(1, testOrderDatabaseAccessor.getOrders("BTCUSD", true).filter { it.clientId == "Client2" }.size)
        // check order is removed from clientOrdersMap
        assertEquals(1, genericLimitOrderService.getAllPreviousOrders("Client2", "BTCUSD", true).size)

        // EURUSD
        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).filter { it.clientId == "Client1" }.size)
        assertFalse(testOrderDatabaseAccessor.getOrders("EURUSD", false).any { it.clientId == "Client1" })

        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).filter { it.clientId == "TrustedClient" }.size)
        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", false).filter { it.clientId == "TrustedClient" }.size)

        assertFalse(testOrderDatabaseAccessor.getOrders("EURUSD", true).any { it.clientId == "Client2" })
        assertFalse(testOrderDatabaseAccessor.getOrders("EURUSD", false).any { it.clientId == "Client2" })

        // check order is removed from ordersMap
        assertNull(genericLimitOrderService.cancelLimitOrder("order1", false))
        assertNull(genericLimitOrderService.cancelLimitOrder("order2", false))

        assertEquals(1, trustedClientsLimitOrdersQueue.size)
        assertEquals(1, (trustedClientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)
        assertEquals(11000.0, (trustedClientsLimitOrdersQueue.first() as LimitOrdersReport).orders.first().order.price)

        assertEquals(1, clientsLimitOrdersQueue.size)
        assertEquals(5, (clientsLimitOrdersQueue.first() as LimitOrdersReport).orders.size)

        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())

        assertEquals(4, rabbitOrderBookQueue.size)
        assertEquals(4, orderBookQueue.size)
    }

    @Test
    fun testCancelOrdersWithRemovedAssetPair() {
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(uid = "order1", clientId = "Client1", assetId = "BTCEUR", price = 10000.0, volume = -1.0)))
        multiLimitOrderService.processMessage(buildMultiLimitOrderWrapper("BTCEUR", "TrustedClient", listOf(VolumePrice(-1.0, price = 10000.0)), emptyList(), emptyList(), listOf("order2")))

        assertEquals(0.0, balancesHolder.getReservedBalance("TrustedClient", "BTC"))
        assertEquals(1.0, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(2, testOrderDatabaseAccessor.getOrders("BTCEUR", false).size)
        assertEquals(2, genericLimitOrderService.getOrderBook("BTCEUR").getOrderBook(false).size)

        testDictionariesDatabaseAccessor.clear() // remove asset pair BTCEUR
        initServices()
        canceller.cancel()

        assertEquals(0, testOrderDatabaseAccessor.getOrders("BTCEUR", false).size)
        assertEquals(0, genericLimitOrderService.getOrderBook("BTCEUR").getOrderBook(false).size)

        // check order is removed from ordersMap
        assertNull(genericLimitOrderService.cancelLimitOrder("order1", false))
        assertNull(genericLimitOrderService.cancelLimitOrder("order2", false))

        // check order is removed from clientOrdersMap
        assertEquals(0, genericLimitOrderService.getAllPreviousOrders("Client1", "BTCEUR", false).size)
        assertEquals(0, genericLimitOrderService.getAllPreviousOrders("TrustedClient", "BTCEUR", false).size)

        assertEquals(1.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(1.0, balancesHolder.getReservedBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("TrustedClient", "BTC"))
        assertEquals(0.0, balancesHolder.getReservedBalance("TrustedClient", "BTC"))

        // recalculate reserved volumes to reset locked reservedAmount
        val recalculator = ReservedVolumesRecalculator(testDictionariesDatabaseAccessor,
                testBackOfficeDatabaseAccessor,
                testOrderDatabaseAccessor,
                TestStopOrderBookDatabaseAccessor(),
                TestReservedVolumesDatabaseAccessor(),
                applicationContext)

        recalculator.recalculate()
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
    }

}