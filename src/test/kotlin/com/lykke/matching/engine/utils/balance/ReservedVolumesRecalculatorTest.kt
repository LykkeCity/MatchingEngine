package com.lykke.matching.engine.utils.balance

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestReservedVolumesDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class ReservedVolumesRecalculatorTest {

    private val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val orderBookDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private val reservedVolumesDatabaseAccessor = TestReservedVolumesDatabaseAccessor()

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))

        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("trustedClient", "BTC", balance = 10.0, reservedBalance = 2.0))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", balance = 10.0, reservedBalance = 1.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "BTC", balance = 10.0, reservedBalance = 2.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "EUR", balance = 10.0, reservedBalance = 3.0))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", balance = 10.0, reservedBalance = 0.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "BTC", balance = 10.0, reservedBalance = 1.0))

        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "trustedClient", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.5))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client1", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.5))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "1", clientId = "Client1", assetId = "EURUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.4))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(uid = "2", clientId = "Client1", assetId = "EURUSD", price = 10000.0, volume = -1.0, reservedVolume = 0.3))
        orderBookDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "BTCUSD", price = 10000.0, volume = -1.0, reservedVolume = 1.0))
    }

    @Test
    fun testRecalculateTrusted() {
        val recalculator = ReservedVolumesRecalculator(testWalletDatabaseAccessor, testBackOfficeDatabaseAccessor, orderBookDatabaseAccessor, reservedVolumesDatabaseAccessor, setOf("trustedClient"))
        recalculator.recalculate()

        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("trustedClient", "BTC"))
        assertEquals(0.5, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(0.7, testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))
        assertEquals(1.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))
        assertEquals(0.0, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))

        assertEquals(4, reservedVolumesDatabaseAccessor.corrections.size)
        assertEquals("1,2", reservedVolumesDatabaseAccessor.corrections.first { it.newReserved == 0.7 }.orderIds)
    }
}