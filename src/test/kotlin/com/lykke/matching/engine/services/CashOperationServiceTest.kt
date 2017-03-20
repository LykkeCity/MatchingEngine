package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.notification.BalanceUpdateNotification
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
    var testBackOfficeDatabaseAcessor = TestBackOfficeDatabaseAccessor()
    val transactionQueue = LinkedBlockingQueue<Transaction>()
    val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAcessor, 60000))
    val balancesHolder = BalancesHolder(testDatabaseAccessor, assetsHolder, balanceNotificationQueue)
    val DELTA = 1e-15

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testBackOfficeDatabaseAcessor.addAsset(Asset("Asset1", 2, "Asset1"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("Asset2", 2, "Asset2"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("Asset3", 2, "Asset3"))
        testBackOfficeDatabaseAcessor.addAsset(Asset("Asset4", 2, "Asset4"))
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset1", 100.0))
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "Asset1", 100.0))
        transactionQueue.clear()
    }

    @Test
    fun testCashIn() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue, balancesHolder)
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
    fun testSmallCashIn() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue, balancesHolder)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", 0.01))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(100.01, balance, DELTA)

        val cashInTransaction = transactionQueue.take() as CashIn
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals(0.01, cashInTransaction.Amount, DELTA)
        assertEquals("Asset1", cashInTransaction.Currency)
    }

    @Test
    fun testCashOut() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue, balancesHolder)
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
    fun testCashOutNegative() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue, balancesHolder)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -50.0))
        var balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(50.0, balance, DELTA)

        val cashOutTransaction = transactionQueue.take() as CashOut
        assertEquals("Client1", cashOutTransaction.clientId)
        assertEquals(50.0, cashOutTransaction.Amount, DELTA)
        assertEquals("Asset1", cashOutTransaction.Currency)

        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -60.0))
        balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertEquals(50.0, balance, DELTA)
    }

    @Test
    fun testResendCashIn() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue, balancesHolder)
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
        val service = CashOperationService(testDatabaseAccessor, transactionQueue, balancesHolder)
        service.processMessage(buildBalanceWrapper("Client1", "Asset4", 100.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset4")

        assertNotNull(balance)
        assertEquals(100.0, balance, DELTA)
    }

    @Test
    fun testAddNewWallet() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue, balancesHolder)
        service.processMessage(buildBalanceWrapper("Client3", "Asset2", 100.0))
        val balance = testDatabaseAccessor.getBalance("Client3", "Asset2")

        assertNotNull(balance)
        assertEquals(100.0, balance, DELTA)
    }

    @Test
    fun testUpdateBalance() {
        val updateService = BalanceUpdateService(balancesHolder)

        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(999.0, balance, DELTA)

    }

    @Test
    fun testRounding() {
        val service = CashOperationService(testDatabaseAccessor, transactionQueue, balancesHolder)
        val updateService = BalanceUpdateService(balancesHolder)

        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 29.99))
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -0.01))

        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")

        assertNotNull(balance)

        assertEquals("29.98", balance.toString())
    }

    private fun buildBalanceWrapper(clientId: String, assetId: String, amount: Double, bussinesId: String = UUID.randomUUID().toString()): MessageWrapper {
        return MessageWrapper("Test", MessageType.CASH_OPERATION.type, ProtocolMessages.CashOperation.newBuilder()
                .setUid(123)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setAmount(amount)
                .setTimestamp(123)
                .setSendToBitcoin(true)
                .setBussinesId(bussinesId).build().toByteArray(), null)
    }

    private fun buildBalanceUpdateWrapper(clientId: String, assetId: String, amount: Double): MessageWrapper {
        return MessageWrapper("Test", MessageType.BALANCE_UPDATE.type, ProtocolMessages.BalanceUpdate.newBuilder()
                .setUid(123)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setAmount(amount).build().toByteArray(), null)
    }
}