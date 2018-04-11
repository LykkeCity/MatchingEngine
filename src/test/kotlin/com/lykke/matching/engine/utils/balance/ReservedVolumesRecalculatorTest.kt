package com.lykke.matching.engine.utils.balance

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class ReservedVolumesRecalculatorTest {

    private val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    private val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val orderBookDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private val stopOrderBookDatabaseAccessor = TestStopOrderBookDatabaseAccessor()
    private val reservedVolumesDatabaseAccessor = TestReservedVolumesDatabaseAccessor()
    val configDatabaseAccessor = TestSettingsDatabaseAccessor()

    private val applicationSettingsCache = ApplicationSettingsCache(configDatabaseAccessor)

    @Before
    fun setUp() {
        configDatabaseAccessor.addTrustedClient("trustedClient")
        configDatabaseAccessor.addTrustedClient("trustedClient2")

        applicationSettingsCache.update()

        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))

        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

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

        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "trustedClient", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.5))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.5))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "1", clientId = "Client1", assetId = "EURUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.4))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "2", clientId = "Client1", assetId = "EURUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.3))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 1.0))

        stopOrderBookDatabaseAccessor.addStopLimitOrder(buildLimitOrder(uid = "3", clientId = "Client2", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = 0.1, lowerLimitPrice = 9000.0, lowerPrice = 9900.0, reservedVolume = 990.0))
        stopOrderBookDatabaseAccessor.addStopLimitOrder(buildLimitOrder(uid = "4", clientId = "Client2", assetId = "BTCUSD", type = LimitOrderType.STOP_LIMIT, volume = 0.1, lowerLimitPrice = 10000.0, lowerPrice = 10900.0))
    }

    @Test
    fun testRecalculate() {
        val recalculator = ReservedVolumesRecalculator(testWalletDatabaseAccessor, testDictionariesDatabaseAccessor,
                testBackOfficeDatabaseAccessor, orderBookDatabaseAccessor, stopOrderBookDatabaseAccessor, reservedVolumesDatabaseAccessor, applicationSettingsCache)
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