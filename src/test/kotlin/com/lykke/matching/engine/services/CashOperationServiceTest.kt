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
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue

class CashOperationServiceTest {

    val testDatabaseAccessor = TestWalletDatabaseAccessor()
    val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    val transactionQueue = LinkedBlockingQueue<JsonSerializable>()
    val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    val balancesHolder = BalancesHolder(testDatabaseAccessor, assetsHolder, balanceNotificationQueue, balanceUpdateQueue, emptySet())
    val DELTA = 1e-15

    @Before
    fun setUp() {
        testDatabaseAccessor.clear()
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset1", 2, "Asset1"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset2", 2, "Asset2"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset3", 2, "Asset3"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset4", 2, "Asset4"))
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset5", 8, "Asset5"))
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset1", 100.0))
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "Asset1", 100.0))
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "Asset1", 100.0, reservedBalance = 50.0))
        transactionQueue.clear()
    }

    @Test
    fun testCashIn() {
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", 50.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(150.0, balance, DELTA)

        val cashInTransaction = transactionQueue.take() as CashOperation
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("50.00", cashInTransaction.volume)
        assertEquals("Asset1", cashInTransaction.asset)
    }

    @Test
    fun testReservedCashIn() {
        val service = ReservedCashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 50.0))
        val balance = testDatabaseAccessor.getBalance("Client3", "Asset1")
        val reservedBalance = testDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(100.0, balance, DELTA)
        assertEquals(100.0, reservedBalance, DELTA)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("50.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testSmallCashIn() {
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", 0.01))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(100.01, balance, DELTA)

        val cashInTransaction = transactionQueue.take() as CashOperation
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("0.01", cashInTransaction.volume)
        assertEquals("Asset1", cashInTransaction.asset)
    }

    @Test
    fun testSmallReservedCashIn() {
        val service = ReservedCashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 0.01))
        val reservedBalance = testDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(50.01, reservedBalance, DELTA)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("0.01", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testCashOut() {
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -50.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(50.0, balance, DELTA)

        val cashOutTransaction = transactionQueue.take() as CashOperation
        assertEquals("Client1", cashOutTransaction.clientId)
        assertEquals("-50.00", cashOutTransaction.volume)
        assertEquals("Asset1", cashOutTransaction.asset)
    }

    @Test
    fun testReservedCashOut() {
        val service = ReservedCashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -49.0))
        val reservedBalance = testDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(1.0, reservedBalance, DELTA)
        val balance = testDatabaseAccessor.getBalance("Client3", "Asset1")
        assertEquals(100.0, balance, DELTA)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("-49.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testCashOutNegative() {
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -50.0))
        var balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(50.0, balance, DELTA)

        val cashOutTransaction = transactionQueue.take() as CashOperation
        assertEquals("Client1", cashOutTransaction.clientId)
        assertEquals("-50.00", cashOutTransaction.volume)
        assertEquals("Asset1", cashOutTransaction.asset)

        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -60.0))
        balance = testDatabaseAccessor.getBalance("Client1", "Asset1")
        assertEquals(50.0, balance, DELTA)
    }

    @Test
    fun testReservedCashOutNegative() {
        val service = ReservedCashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -24.0))
        var reservedBalance = testDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(26.0, reservedBalance, DELTA)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("-24.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)

        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -30.0))
        reservedBalance = testDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(26.0, reservedBalance, DELTA)
    }

    @Test
    fun testReservedCashInHigherThatBalance() {
        val service = ReservedCashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 50.01))
        val reservedBalance = testDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(50.0, reservedBalance, DELTA)
    }

    @Test
    fun testResendCashIn() {
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
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

        val cashInTransaction = transactionQueue.peek() as CashOperation
        assertNotNull(cashInTransaction)
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("50.00", cashInTransaction.volume)
        assertEquals("Asset1", cashInTransaction.asset)
    }

    @Test
    fun testResendReservedCashIn() {
        val service = ReservedCashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 10.0, "TestId"))
        var reservedBalance = testDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(60.0, reservedBalance, DELTA)

        val externalOperation = testDatabaseAccessor.loadExternalCashOperation("Client3", "TestId")
        assertNotNull(externalOperation)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertNotNull(operation)
        assertEquals("Client3", operation.clientId)
        assertEquals("10.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)

        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 10.0, "TestId"))
        reservedBalance = testDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(60.0, reservedBalance, DELTA)

        assertEquals(0, transactionQueue.size)
    }

    @Test
    fun testAddNewAsset() {
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildBalanceWrapper("Client1", "Asset4", 100.0))
        val balance = testDatabaseAccessor.getBalance("Client1", "Asset4")

        assertNotNull(balance)
        assertEquals(100.0, balance, DELTA)
    }

    @Test
    fun testAddNewWallet() {
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
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
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)
        val updateService = BalanceUpdateService(balancesHolder)

        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 29.99))
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -0.01))

        val balance = testDatabaseAccessor.getBalance("Client1", "Asset1")

        assertNotNull(balance)

        assertEquals("29.98", balance.toString())
    }

    @Test
    fun testRoundingWithReserved() {
        testDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset5", 1.00418803, 0.00418803))
        val service = CashInOutOperationService(testDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue)

        service.processMessage(buildBalanceWrapper("Client1", "Asset5", -1.0))

        assertEquals(1, transactionQueue.size)
        val cashInTransaction = transactionQueue.peek() as CashOperation
        assertNotNull(cashInTransaction)
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("-1.00000000", cashInTransaction.volume)
        assertEquals("Asset5", cashInTransaction.asset)

    }

    private fun buildBalanceWrapper(clientId: String, assetId: String, amount: Double, bussinesId: String = UUID.randomUUID().toString()): MessageWrapper {
        return MessageWrapper("Test", MessageType.CASH_IN_OUT_OPERATION.type, ProtocolMessages.CashInOutOperation.newBuilder()
                .setId(bussinesId)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setVolume(amount)
                .setTimestamp(Date().time).build().toByteArray(), null)
    }

    private fun buildBalanceUpdateWrapper(clientId: String, assetId: String, amount: Double): MessageWrapper {
        return MessageWrapper("Test", MessageType.OLD_BALANCE_UPDATE.type, ProtocolMessages.OldBalanceUpdate.newBuilder()
                .setUid(123)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setAmount(amount).build().toByteArray(), null)
    }

    private fun buildReservedCashInOutWrapper(clientId: String, assetId: String, amount: Double, bussinesId: String = UUID.randomUUID().toString()): MessageWrapper {
        return MessageWrapper("Test", MessageType.RESERVED_CASH_IN_OUT_OPERATION.type, ProtocolMessages.ReservedCashInOutOperation.newBuilder()
                .setId(bussinesId)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setReservedVolume(amount)
                .setTimestamp(Date().time).build().toByteArray(), null)
    }
}