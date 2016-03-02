package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Wallet
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.messages.ProtocolMessages
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Before
import org.junit.Test

class CashOperationServiceTest {

    val testDatabaseAccessor = TestWalletDatabaseAccessor()
    val DELTA = 1e-15

    @Before
    fun setUp() {
        testDatabaseAccessor.insertOrUpdateWallet(Wallet("Client1", "Asset1", 100.0))
        testDatabaseAccessor.insertOrUpdateWallet(Wallet("Client2", "Asset1", 100.0))
    }

    @Test
    fun testChangeBalance() {
        val service = CashOperationService(testDatabaseAccessor)
        service.processMessage(buildByteArray("Client1", "Asset1", -50.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(50.0, balance!!, DELTA)

        val operation = testDatabaseAccessor.operations.find { it.getClientId() == ("Client1") }
        assertNotNull(operation)
        assertEquals(-50.0, operation!!.amount, DELTA)
    }

    @Test
    fun testAddNewAsset() {
        val service = CashOperationService(testDatabaseAccessor)
        service.processMessage(buildByteArray("Client1", "Asset4", 100.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset4")

        assertNotNull(balance)
        assertEquals(100.0, balance!!, DELTA)

        val operation = testDatabaseAccessor.operations.find { it.getClientId() == ("Client1") }
        assertNotNull(operation)
        assertEquals(100.0, operation!!.amount, DELTA)
    }

    @Test
    fun testAddNewWallet() {
        val service = CashOperationService(testDatabaseAccessor)
        service.processMessage(buildByteArray("Client3", "Asset2", 100.0))
        val balance = testDatabaseAccessor.getBalance("Client3", "Asset2")

        assertNotNull(balance)
        assertEquals(100.0, balance!!, DELTA)

        val operation = testDatabaseAccessor.operations.find { it.getClientId() == ("Client3") }
        assertNotNull(operation)
        assertEquals(100.0, operation!!.amount, DELTA)
    }

    private fun buildByteArray(clientId: String, assetId: String, amount: Double): ByteArray {
        return ProtocolMessages.CashOperation.newBuilder()
                .setUid(123)
                .setAccountId(clientId)
                .setAssetId(assetId)
                .setAmount(amount)
                .setDate(123).build().toByteArray()
    }
}