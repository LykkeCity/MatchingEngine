package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.fee.NewFeeInstruction
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestCashOperationsDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.messages.ProtocolMessages
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildFeeInstruction
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildFeeInstructions
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue

class CashOperationServiceTest {

    private var testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    private var testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private val testCashOperationsDatabaseAccessor = TestCashOperationsDatabaseAccessor()
    private val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val transactionQueue = LinkedBlockingQueue<JsonSerializable>()
    private val balanceNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    private val assetsPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor, 60000)
    private val assetsPairsHolder = AssetsPairsHolder(assetsPairsCache)
    private lateinit var balancesHolder: BalancesHolder
    private val testFileOrderDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private lateinit var genericLimitOrderService: GenericLimitOrderService
    private val DELTA = 1e-15
    private lateinit var feeProcessor: FeeProcessor
    private lateinit var service: CashInOutOperationService

    @Before
    fun setUp() {
        testWalletDatabaseAccessor.clear()
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset1", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset2", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset3", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset4", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("Asset5", 8))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset1", 100.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "Asset1", 100.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "Asset1", 100.0, reservedBalance = 50.0))
        transactionQueue.clear()
        initService()
    }

    private fun initService() {
        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, balanceNotificationQueue, balanceUpdateQueue, emptySet())
        assetsPairsCache.update()
        genericLimitOrderService = GenericLimitOrderService(testFileOrderDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, LinkedBlockingQueue(), LinkedBlockingQueue(), emptySet())
        feeProcessor = FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)
        service = CashInOutOperationService(testCashOperationsDatabaseAccessor, assetsHolder, balancesHolder, transactionQueue, feeProcessor)
    }

    @Test
    fun testCashIn() {
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", 50.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(150.0, balance, DELTA)

        val cashInTransaction = transactionQueue.take() as CashOperation
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("50.00", cashInTransaction.volume)
        assertEquals("Asset1", cashInTransaction.asset)
    }

    @Test
    fun testReservedCashIn() {
        val service = ReservedCashInOutOperationService(assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 50.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client3", "Asset1")
        val reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(100.0, balance, DELTA)
        assertEquals(100.0, reservedBalance, DELTA)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("50.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testSmallCashIn() {
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", 0.01))
        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(100.01, balance, DELTA)

        val cashInTransaction = transactionQueue.take() as CashOperation
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("0.01", cashInTransaction.volume)
        assertEquals("Asset1", cashInTransaction.asset)
    }

    @Test
    fun testSmallReservedCashIn() {
        val service = ReservedCashInOutOperationService(assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 0.01))
        val reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(50.01, reservedBalance, DELTA)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("0.01", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testCashOut() {
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -50.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(50.0, balance, DELTA)

        val cashOutTransaction = transactionQueue.take() as CashOperation
        assertEquals("Client1", cashOutTransaction.clientId)
        assertEquals("-50.00", cashOutTransaction.volume)
        assertEquals("Asset1", cashOutTransaction.asset)
    }

    @Test
    fun testReservedCashOut() {
        val service = ReservedCashInOutOperationService(assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -49.0))
        val reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(1.0, reservedBalance, DELTA)
        val balance = testWalletDatabaseAccessor.getBalance("Client3", "Asset1")
        assertEquals(100.0, balance, DELTA)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("-49.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)
    }

    @Test
    fun testCashOutNegative() {
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -50.0))
        var balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(50.0, balance, DELTA)

        val cashOutTransaction = transactionQueue.take() as CashOperation
        assertEquals("Client1", cashOutTransaction.clientId)
        assertEquals("-50.00", cashOutTransaction.volume)
        assertEquals("Asset1", cashOutTransaction.asset)

        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -60.0))
        balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertEquals(50.0, balance, DELTA)
    }

    @Test
    fun testReservedCashOutNegative() {
        val service = ReservedCashInOutOperationService(assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -24.0))
        var reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(26.0, reservedBalance, DELTA)

        val operation = transactionQueue.take() as ReservedCashOperation
        assertEquals("Client3", operation.clientId)
        assertEquals("-24.00", operation.reservedVolume)
        assertEquals("Asset1", operation.asset)

        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", -30.0))
        reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(26.0, reservedBalance, DELTA)
    }

    @Test
    fun testReservedCashInHigherThanBalance() {
        val service = ReservedCashInOutOperationService(assetsHolder, balancesHolder, transactionQueue)
        service.processMessage(buildReservedCashInOutWrapper("Client3", "Asset1", 50.01))
        val reservedBalance = testWalletDatabaseAccessor.getReservedBalance("Client3", "Asset1")
        assertEquals(50.0, reservedBalance, DELTA)
    }

    @Test
    fun testAddNewAsset() {
        service.processMessage(buildBalanceWrapper("Client1", "Asset4", 100.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset4")

        assertNotNull(balance)
        assertEquals(100.0, balance, DELTA)
    }

    @Test
    fun testAddNewWallet() {
        service.processMessage(buildBalanceWrapper("Client3", "Asset2", 100.0))
        val balance = testWalletDatabaseAccessor.getBalance("Client3", "Asset2")

        assertNotNull(balance)
        assertEquals(100.0, balance, DELTA)
    }

    @Test
    fun testUpdateBalance() {
        val updateService = BalanceUpdateService(balancesHolder)

        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 999.0))

        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")
        assertNotNull(balance)
        assertEquals(999.0, balance, DELTA)

    }

    @Test
    fun testRounding() {
        val updateService = BalanceUpdateService(balancesHolder)

        updateService.processMessage(buildBalanceUpdateWrapper("Client1", "Asset1", 29.99))
        service.processMessage(buildBalanceWrapper("Client1", "Asset1", -0.01))

        val balance = testWalletDatabaseAccessor.getBalance("Client1", "Asset1")

        assertNotNull(balance)

        assertEquals("29.98", balance.toString())
    }

    @Test
    fun testRoundingWithReserved() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset5", 1.00418803, 0.00418803))
        initService()

        service.processMessage(buildBalanceWrapper("Client1", "Asset5", -1.0))

        assertEquals(1, transactionQueue.size)
        val cashInTransaction = transactionQueue.peek() as CashOperation
        assertNotNull(cashInTransaction)
        assertEquals("Client1", cashInTransaction.clientId)
        assertEquals("-1.00000000", cashInTransaction.volume)
        assertEquals("Asset5", cashInTransaction.asset)

    }

    @Test
    fun testCashOutFee() {
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("AssetPair", "Asset5", "Asset4", 2))
        testFileOrderDatabaseAccessor.addLimitOrder(buildLimitOrder(clientId = "Client2", assetId = "AssetPair", volume = 1.0, price = 2.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset4", 0.06, 0.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset5", 11.0, 0.0))
        initService()
        service.processMessage(buildBalanceWrapper("Client1", "Asset5", -1.0,
                fees = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.1, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3", assetIds = listOf("Asset4"))))

        assertEquals(0.01, balancesHolder.getBalance("Client1", "Asset4"), DELTA)
        assertEquals(0.05, balancesHolder.getBalance("Client3", "Asset4"), DELTA)
        assertEquals(10.0, balancesHolder.getBalance("Client1", "Asset5"), DELTA)
    }

    @Test
    fun testCashOutInvalidFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "Asset5", 3.0, 0.0))
        initService()

        // Negative fee size
        service.processMessage(buildBalanceWrapper("Client1", "Asset5", -1.0,
                fees = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = -0.1, sizeType = FeeSizeType.PERCENTAGE, targetClientId = "Client3")))

        assertEquals(3.0, balancesHolder.getBalance("Client1", "Asset5"), DELTA)
        assertEquals(0.0, balancesHolder.getBalance("Client3", "Asset5"), DELTA)

        // Fee amount is more than operation amount
        service.processMessage(buildBalanceWrapper("Client1", "Asset5", -0.9,
                fees = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.9, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3")))

        // Multiple fee amount is more than operation amount
        service.processMessage(buildBalanceWrapper("Client1", "Asset5", -1.0,
                fees = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.5, sizeType = FeeSizeType.PERCENTAGE, targetClientId = "Client3")!!,
                        buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.5, sizeType = FeeSizeType.PERCENTAGE, targetClientId = "Client3")!!)))

        assertEquals(3.0, balancesHolder.getBalance("Client1", "Asset5"), DELTA)
        assertEquals(0.0, balancesHolder.getBalance("Client3", "Asset5"), DELTA)
    }

    private fun buildBalanceWrapper(clientId: String, assetId: String, amount: Double, bussinesId: String = UUID.randomUUID().toString(),
                                    fees: List<NewFeeInstruction> = listOf()): MessageWrapper {
        val builder = ProtocolMessages.CashInOutOperation.newBuilder()
                .setId(bussinesId)
                .setClientId(clientId)
                .setAssetId(assetId)
                .setVolume(amount)
                .setTimestamp(Date().time)
        fees.forEach {
            builder.addFees(MessageBuilder.buildFee(it))
        }
        return MessageWrapper("Test", MessageType.CASH_IN_OUT_OPERATION.type, builder.build().toByteArray(), null)
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