package com.lykke.matching.engine.utils.balance

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
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

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (ReservedVolumesRecalculatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ReservedVolumesRecalculatorTest {

    private val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    private val orderBookDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private val stopOrderBookDatabaseAccessor = TestStopOrderBookDatabaseAccessor()
    private val reservedVolumesDatabaseAccessor = TestReservedVolumesDatabaseAccessor()


    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))

            return testBackOfficeDatabaseAccessor
        }

        @Bean
        @Primary
        open fun testWalledDatabaseAccessor(): WalletDatabaseAccessor {
            val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()

            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("trustedClient", "BTC", balance = 10.0, reservedBalance = 2.0))
            // negative reserved balance
            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("trustedClient2", "BTC", balance = 1.0, reservedBalance = -0.001))
            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "BTC", balance = 0.0, reservedBalance = -0.001))

            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", balance = 10.0, reservedBalance = 1.0))
            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", balance = 10.0, reservedBalance = 2.0))
            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", balance = 10.0, reservedBalance = 3.0))

            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", balance = 10.0, reservedBalance = 0.0))
            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", balance = 10.0, reservedBalance = 1.0))
            testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", balance = 990.0, reservedBalance = 1.0))

            return testWalletDatabaseAccessor
        }

        @Bean
        @Primary
        open fun testConfig(): TestConfigDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestConfigDatabaseAccessor()
            testSettingsDatabaseAccessor.addTrustedClient("trustedClient")
            testSettingsDatabaseAccessor.addTrustedClient("trustedClient2")
            return testSettingsDatabaseAccessor
        }
    }

    @Autowired
    lateinit var applicationContext: ApplicationContext

    @Autowired
    lateinit var testWalletDatabaseAccessor: TestWalletDatabaseAccessor

    @Autowired
    lateinit var testBackOfficeDatabaseAccessor: TestBackOfficeDatabaseAccessor

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "trustedClient", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.5))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.5))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "1", clientId = "Client1", assetId = "EURUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.4))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "2", clientId = "Client1", assetId = "EURUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.3))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 1.0))

        stopOrderBookDatabaseAccessor.addStopLimitOrder(buildLimitOrder(uid = "3", clientId = "Client2", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = 0.1, lowerLimitPrice = 9000.0, lowerPrice = 9900.0, reservedVolume = 990.0))
        stopOrderBookDatabaseAccessor.addStopLimitOrder(buildLimitOrder(uid = "4", clientId = "Client2", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = 0.1, lowerLimitPrice = 10000.0, lowerPrice = 10900.0))
    }

    @Before
    fun init() {
        stopOrderBookDatabaseAccessor.addStopLimitOrder(buildLimitOrder(uid = "3", clientId = "Client2", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = 0.1, lowerLimitPrice = 9000.0, lowerPrice = 9900.0, reservedVolume = 990.0))
        stopOrderBookDatabaseAccessor.addStopLimitOrder(buildLimitOrder(uid = "4", clientId = "Client2", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = 0.1, lowerLimitPrice = 10000.0, lowerPrice = 10900.0))
    }

    @Test
    fun testRecalculate() {
        val recalculator = ReservedVolumesRecalculator(testWalletDatabaseAccessor,
                testDictionariesDatabaseAccessor, testBackOfficeDatabaseAccessor, orderBookDatabaseAccessor,
                stopOrderBookDatabaseAccessor, reservedVolumesDatabaseAccessor, applicationContext)
        recalculator.recalculate()

        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("trustedClient", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("trustedClient2", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client3", "BTC"))
        assertEquals(0.5, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(0.7, testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))
        assertEquals(1.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))
        assertEquals(2080.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))

        assertEquals(7, reservedVolumesDatabaseAccessor.corrections.size)
        assertEquals("1,2", reservedVolumesDatabaseAccessor.corrections.first { it.newReserved == 0.7 }.orderIds)
        assertEquals("3,4", reservedVolumesDatabaseAccessor.corrections.first { it.newReserved == 2080.0 }.orderIds)
    }
}