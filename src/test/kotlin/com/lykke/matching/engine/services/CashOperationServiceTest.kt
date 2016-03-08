package com.lykke.matching.engine.services

import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
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
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset1", 100.0))
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "Asset1", 100.0))
    }

    @Test
    fun testChangeBalance() {
        val service = CashOperationService(testDatabaseAccessor)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -50.0))
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
        service.processMessage(buildBalanceWrapper("Client1", "Asset4", 100.0))
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
        service.processMessage(buildBalanceWrapper("Client3", "Asset2", 100.0))
        val balance = testDatabaseAccessor.getBalance("Client3", "Asset2")

        assertNotNull(balance)
        assertEquals(100.0, balance!!, DELTA)

        val operation = testDatabaseAccessor.operations.find { it.getClientId() == ("Client3") }
        assertNotNull(operation)
        assertEquals(100.0, operation!!.amount, DELTA)
    }

    private fun buildBalanceWrapper(clientId: String, assetId: String, amount: Double): MessageWrapper {
        return MessageWrapper(MessageType.UPDATE_BALANCE, ProtocolMessages.CashOperation.newBuilder()
                .setUid(123)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setAmount(amount)
                .setDateTime(123).build().toByteArray(), null)
    }
}