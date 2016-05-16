package com.lykke.matching.engine.services

import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.queue.transaction.CashIn
import com.lykke.matching.engine.queue.transaction.CashOut
import com.lykke.matching.engine.queue.transaction.Transaction
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Before
import org.junit.Test
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue

class CashOperationServiceTest {

    val testDatabaseAccessor = TestWalletDatabaseAccessor()
    val transactionQueue = LinkedBlockingQueue<Transaction>()
    val DELTA = 1e-15

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset1", 100.0))
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "Asset1", 100.0))
        transactionQueue.clear()
    }

    @Test
    fun testCashIn() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", 50.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(150.0, balance, DELTA)

        val cashInTransaction = transactionQueue.take() as CashIn
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals(50.0, cashInTransaction.Amount, DELTA)
        assertEquals("Asset1", cashInTransaction.Currency)
    }

    @Test
    fun testCashOut() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -50.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(50.0, balance, DELTA)

        val cashOutTransaction = transactionQueue.take() as CashOut
        assertEquals("Client1", cashOutTransaction.clientId)
        assertEquals(50.0, cashOutTransaction.Amount, DELTA)
        assertEquals("Asset1", cashOutTransaction.Currency)
    }

    @Test
    fun testResendCashIn() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", 50.0, "TestId"))
        var balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(150.0, balance, DELTA)

        val externalOperation = testDatabaseAccessor.loadExternalCashOperation("Client1", "TestId")
        assertNotNull(externalOperation)

        service.processMessage(buildBalanceWrapper("Client1", "Asset1", 50.0, "TestId"))
        balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(150.0, balance, DELTA)

        val cashInTransaction = transactionQueue.peek() as CashIn
        assertNotNull(cashInTransaction)
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals(50.0, cashInTransaction.Amount, DELTA)
        assertEquals("Asset1", cashInTransaction.Currency)
    }

    @Test
    fun testAddNewAsset() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset4", 100.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset4")

        assertNotNull(balance)
        assertEquals(100.0, balance, DELTA)
    }

    @Test
    fun testAddNewWallet() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client3", "Asset2", 100.0))
        val balance = testDatabaseAccessor.getBalance("Client3", "Asset2")

        assertNotNull(balance)
        assertEquals(100.0, balance, DELTA)
    }

    @Test
    fun testUpdateBalance() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue)
        val updateService = BalanceUpdateService(service)

        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(999.0, balance, DELTA)

    }

    private fun buildBalanceWrapper(clientId: String, assetId: String, amount: Double, bussinesId: String = UUID.randomUUID().toString()): MessageWrapper {
        return MessageWrapper(MessageType.CASH_OPERATION, ProtocolMessages.CashOperation.newBuilder()
                .setUid(123)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setAmount(amount)
                .setDateTime(123)
                .setSendToBitcoin(true)
                .setBussinesId(bussinesId).build().toByteArray(), null)
    }

    private fun buildBalanceUpdateWrapper(clientId: String, assetId: String, amount: Double): MessageWrapper {
        return MessageWrapper(MessageType.BALANCE_UPDATE, ProtocolMessages.BalanceUpdate.newBuilder()
                .setUid(123)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setAmount(amount).build().toByteArray(), null)
    }
}