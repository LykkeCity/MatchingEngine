package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.notification.TestReservedCashOperationListener
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildCashInOutWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildFeeInstruction
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildFeeInstructions
import org.junit.Assert.assertEquals
import com.lykke.matching.engine.utils.assertEquals
import org.junit.Assert.assertNotNull
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
import java.util.Date
import java.util.UUID

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (CashOperationServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashOperationServiceTest: AbstractTest() {

    @Autowired
    private lateinit var testReservedCashOperationListener: TestReservedCashOperationListener

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("Asset1", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("Asset2", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("Asset3", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("Asset4", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("Asset5", 8))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset1", 100.0)
        testBalanceHolderWrapper.updateBalance("Client2", "Asset1", 100.0)
        testBalanceHolderWrapper.updateBalance("Client3", "Asset1", 100.0)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "Asset1", 50.0)

        cashInOutQueue.clear()
        initServices()
    }

    @Test
    fun testCashIn() {
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset1", 50.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(BigDecimal.valueOf(150.0), balance)

        val cashInTransaction = cashInOutQueue.take() as CashOperation
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("50.00", cashInTransaction.volume)
        assertEquals("Asset1", cashInTransaction.asset)
    }

    @Test
    fun testReservedCashIn() {
        reservedCashInOutOperationService.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 50.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client3", "Asset1")
        val reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(BigDecimal.valueOf(100.0), balance)
        assertEquals(BigDecimal.valueOf(100.0), reservedBalance)

        val operation = testReservedCashOperationListener.getQueue().take().reservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("50.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testSmallCashIn() {
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset1", 0.01))
        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(BigDecimal.valueOf(100.01), balance)

        val cashInTransaction = cashInOutQueue.take() as CashOperation
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("0.01", cashInTransaction.volume)
        assertEquals("Asset1", cashInTransaction.asset)
    }

    @Test
    fun testSmallReservedCashIn() {
        reservedCashInOutOperationService.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 0.01))
        val reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(BigDecimal.valueOf(50.01), reservedBalance)

        val operation = testReservedCashOperationListener.getQueue().take().reservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("0.01", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testCashOut() {
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset1", -50.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(BigDecimal.valueOf(50.0), balance)

        val cashOutTransaction = cashInOutQueue.take() as CashOperation
        assertEquals("Client1", cashOutTransaction.clientId)
        assertEquals("-50.00", cashOutTransaction.volume)
        assertEquals("Asset1", cashOutTransaction.asset)
    }

    @Test
    fun testReservedCashOut() {
        reservedCashInOutOperationService.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -49.0))
        val reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(BigDecimal.valueOf(1.0), reservedBalance)
        val balance = testWalletDatabaseAccessor.getBalance("Client3", "Asset1")
        assertEquals(BigDecimal.valueOf(100.0), balance)

        val operation = testReservedCashOperationListener.getQueue().poll().reservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("-49.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testCashOutNegative() {
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset1", -50.0))
        var balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(BigDecimal.valueOf(50.0), balance)

        val cashOutTransaction = cashInOutQueue.take() as CashOperation
        assertEquals("Client1", cashOutTransaction.clientId)
        assertEquals("-50.00", cashOutTransaction.volume)
        assertEquals("Asset1", cashOutTransaction.asset)

        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset1", -60.0))
        balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertEquals(BigDecimal.valueOf(50.0), balance)
    }

    @Test
    fun testReservedCashOutNegative() {
        reservedCashInOutOperationService.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -24.0))
        var reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(BigDecimal.valueOf(26.0), reservedBalance)

        val operation = testReservedCashOperationListener.getQueue().take().reservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("-24.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)

        reservedCashInOutOperationService.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -30.0))
        reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(BigDecimal.valueOf(26.0), reservedBalance)
    }

    @Test
    fun testReservedCashInHigherThanBalance() {
        reservedCashInOutOperationService.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 50.01))
        val reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(BigDecimal.valueOf(50.0), reservedBalance)
    }

    @Test
    fun testAddNewAsset() {
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset4", 100.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset4")

        assertNotNull(balance)
        assertEquals(BigDecimal.valueOf(100.0), balance)
    }

    @Test
    fun testAddNewWallet() {
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client3", "Asset2", 100.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client3", "Asset2")

        assertNotNull(balance)
        assertEquals(BigDecimal.valueOf(100.0), balance)
    }

    @Test
    fun testUpdateBalance() {
        balanceUpdateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(BigDecimal.valueOf(999.0), balance)

    }

    @Test
    fun testRounding() {
        balanceUpdateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 29.99))
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset1", -0.01))

        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")

        assertNotNull(balance)

        assertEquals("29.98", balance.toString())
    }

    @Test
    fun testRoundingWithReserved() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset5", 1.00418803)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "Asset5", 0.00418803)
        initServices()

        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset5", -1.0))

        assertEquals(1, cashInOutQueue.size)
        val cashInTransaction = cashInOutQueue.peek() as CashOperation
        assertNotNull(cashInTransaction)
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("-1.00000000", cashInTransaction.volume)
        assertEquals("Asset5", cashInTransaction.asset)

    }

    @Test
    fun testCashOutFee() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset4", 0.06)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "Asset4",  0.0)
        testBalanceHolderWrapper.updateBalance("Client1", "Asset5", 11.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "Asset5",0.0)
        initServices()
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset5", -1.0,
                fees = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.05, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3", assetIds = listOf("Asset4"))))

        assertEquals(BigDecimal.valueOf(0.01), balancesHolder.getBalance("Client1", "Asset4"))
        assertEquals(BigDecimal.valueOf(0.05), balancesHolder.getBalance("Client3", "Asset4"))
        assertEquals(BigDecimal.valueOf(10.0), balancesHolder.getBalance("Client1", "Asset5"))
    }

    @Test
    fun testCashOutInvalidFee() {
        testBalanceHolderWrapper.updateBalance("Client1", "Asset5", 3.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "Asset5", 0.0)
        initServices()

        // Negative fee size
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset5", -1.0,
                fees = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = -0.1, sizeType = FeeSizeType.PERCENTAGE, targetClientId = "Client3")))

        assertEquals(BigDecimal.valueOf(3.0), balancesHolder.getBalance("Client1", "Asset5"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client3", "Asset5"))

        // Fee amount is more than operation amount
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset5", -0.9,
                fees = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.91, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3")))

        // Multiple fee amount is more than operation amount
        cashInOutOperationService.processMessage(buildCashInOutWrapper("Client1", "Asset5", -1.0,
                fees = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.5, sizeType = FeeSizeType.PERCENTAGE, targetClientId = "Client3")!!,
                        buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.51, sizeType = FeeSizeType.PERCENTAGE, targetClientId = "Client3")!!)))

        assertEquals(BigDecimal.valueOf(3.0), balancesHolder.getBalance("Client1", "Asset5"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client3", "Asset5"))
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