package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildFeeInstruction
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildFeeInstructions
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstruction
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstructions
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.LinkedList
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FeeProcessorTest {

    private var testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    private lateinit var balancesHolder: BalancesHolder
    private lateinit var feeProcessor: FeeProcessor

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))

        initServices()
    }

    private fun initServices() {
        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, emptySet())
        feeProcessor = FeeProcessor(balancesHolder, assetsHolder)
    }

    @Test
    fun testNoPercentageFee() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.0))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.0))
        val originalOperations = LinkedList(operations)
        val receiptOperation = operations[1]

        var feeInstructions = buildFeeInstructions()
        var feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(0, feeTransfers.size)
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.NO_FEE)
        feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(0, feeTransfers.size)
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE)
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.05)
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.0, targetClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 1.01, targetClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.EXTERNAL_FEE)
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, sizeType = null, size = 0.01, targetClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.EXTERNAL_FEE, size = 0.05, sourceClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.EXTERNAL_FEE, size = 0.05, targetClientId = "Client4")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.01, targetClientId = "Client3")
        assertFails { feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)
    }

    @Test
    fun testNoAbsoluteFee() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -0.5))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 0.5))
        val originalOperations = LinkedList(operations)
        val receiptOperation = operations[1]

        val feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.6, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)
    }

    @Test
    fun testClientPercentageFee() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.01, targetClientId = "Client3")
        val feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, feeTransfers.size)
        val feeTransfer = feeTransfers.first()
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client3", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.11, feeTransfer.volume)

        assertEquals(3, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertFalse { operations[0].isFee }
        assertEquals(9.99, operations[1].amount)
        assertFalse { operations[1].isFee }
        assertEquals(0.11, operations[2].amount)
        assertEquals("Client3", operations[2].clientId)
        assertTrue { operations[2].isFee }
    }

    @Test
    fun testClientAbsoluteFee() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -11.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 11.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 1.1, targetClientId = "Client3")
        val feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, feeTransfers.size)
        val feeTransfer = feeTransfers.first()
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client3", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(1.1, feeTransfer.volume)

        assertEquals(3, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertFalse { operations[0].isFee }
        assertEquals(10.0, operations[1].amount)
        assertFalse { operations[1].isFee }
        assertEquals(1.1, operations[2].amount)
        assertEquals("Client3", operations[2].clientId)
        assertTrue { operations[2].isFee }
    }

    @Test
    fun testClientPercentageFeeRound() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -29.99))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 29.99))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.0001, targetClientId = "Client3")
        val feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, feeTransfers.size)
        val feeTransfer = feeTransfers.first()
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client3", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.01, feeTransfer.volume)

        assertEquals(3, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertEquals(29.98, operations[1].amount)
        assertFalse { operations[1].isFee }
        assertEquals(0.01, operations[2].amount)
        assertEquals("Client3", operations[2].clientId)
        assertTrue { operations[2].isFee }
    }

    @Test
    fun testExternalPercentageFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1000.0))
        initServices()

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = buildFeeInstructions(type = FeeType.EXTERNAL_FEE, size = 0.01, sourceClientId = "Client3", targetClientId = "Client4")
        val feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, feeTransfers.size)
        val feeTransfer = feeTransfers.first()
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client3", feeTransfer.fromClientId)
        assertEquals("Client4", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.11, feeTransfer.volume)

        assertEquals(4, operations.size)
        assertEquals(originalOperations, operations.subList(0, 2))
        assertEquals(-0.11, operations[2].amount)
        assertEquals("Client3", operations[2].clientId)
        assertTrue { operations[2].isFee }
        assertEquals(0.11, operations[3].amount)
        assertEquals("Client4", operations[3].clientId)
        assertTrue { operations[3].isFee }
    }

    @Test
    fun testExternalPercentageFeeNotEnoughFunds() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 0.1))
        initServices()

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = buildFeeInstructions(type = FeeType.EXTERNAL_FEE, size = 0.01, sourceClientId = "Client3", targetClientId = "Client4")
        val feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, feeTransfers.size)
        val feeTransfer = feeTransfers.first()
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client4", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.11, feeTransfer.volume)

        assertEquals(3, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertEquals(9.99, operations[1].amount)
        assertFalse { operations[1].isFee }
        assertEquals(0.11, operations[2].amount)
        assertEquals("Client4", operations[2].clientId)
        assertTrue { operations[2].isFee }
    }

    @Test
    fun testMakerPercentageFee() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = buildLimitOrderFeeInstructions(type = FeeType.CLIENT_FEE, takerSize = 0.01, makerSize = 0.02, targetClientId = "Client3")
        val feeTransfers = feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, feeTransfers.size)
        val feeTransfer = feeTransfers.first()
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client3", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.21, feeTransfer.volume)

        assertEquals(3, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertFalse { operations[0].isFee }
        assertEquals(9.89, operations[1].amount)
        assertFalse { operations[1].isFee }
        assertEquals(0.21, operations[2].amount)
        assertEquals("Client3", operations[2].clientId)
        assertTrue { operations[2].isFee }
    }

    @Test
    fun testMakerAbsoluteFee() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = buildLimitOrderFeeInstructions(type = FeeType.CLIENT_FEE,
                takerSize = 0.1,
                makerSizeType = FeeSizeType.ABSOLUTE,
                makerSize = 0.2,
                targetClientId = "Client3")

        val feeTransfers = feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, feeTransfers.size)
        val feeTransfer = feeTransfers.first()
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client3", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.2, feeTransfer.volume)

        assertEquals(3, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertFalse { operations[0].isFee }
        assertEquals(9.9, operations[1].amount)
        assertFalse { operations[1].isFee }
        assertEquals(0.2, operations[2].amount)
        assertEquals("Client3", operations[2].clientId)
        assertTrue { operations[2].isFee }
    }

    @Test
    fun testMakerMultipleFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client4", "USD", 1000.0))

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = listOf(
                buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE, takerSize = 0.01, makerSize = 0.02, targetClientId = "Client3")!!,
                buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE, takerSize = 0.01, makerSize = 0.04, targetClientId = "Client5")!!,
                buildLimitOrderFeeInstruction(type = FeeType.EXTERNAL_FEE, takerSize = 0.01, makerSize = 0.03, sourceClientId = "Client4", targetClientId = "Client3")!!
        )
        val feeTransfers = feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations)

        assertEquals(3, feeTransfers.size)
        var feeTransfer = feeTransfers[0]
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client3", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.21, feeTransfer.volume)

        feeTransfer = feeTransfers[1]
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client5", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.41, feeTransfer.volume)

        feeTransfer = feeTransfers[2]
        assertEquals("USD", feeTransfer.asset)
        assertEquals("Client4", feeTransfer.fromClientId)
        assertEquals("Client3", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.31, feeTransfer.volume)


        assertEquals(6, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertFalse { operations[0].isFee }

        val subOperations = operations.subList(1, operations.size).sortedBy { it.amount }
        assertEquals(-0.31, subOperations[0].amount)
        assertEquals("Client4", subOperations[0].clientId)
        assertTrue { subOperations[0].isFee }

        assertEquals(0.21, subOperations[1].amount)
        assertEquals("Client3", subOperations[1].clientId)
        assertTrue { subOperations[1].isFee }

        assertEquals(0.31, subOperations[2].amount)
        assertEquals("Client3", subOperations[2].clientId)
        assertTrue { subOperations[2].isFee }

        assertEquals(0.41, subOperations[3].amount)
        assertEquals("Client5", subOperations[3].clientId)
        assertTrue { subOperations[3].isFee }

        assertEquals(9.48, subOperations[4].amount)
        assertFalse { subOperations[4].isFee }
    }

    @Test
    fun testExternalMultipleFeeNotEnoughFunds() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 1.12))

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.12))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.12))
        val receiptOperation = operations[1]

        val feeInstructions = listOf(
                buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.03, sourceClientId = "Client3", targetClientId = "Client4")!!,
                buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.03, sourceClientId = "Client3", targetClientId = "Client5")!!,
                buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.05, sourceClientId = "Client3", targetClientId = "Client6")!!
        )
        val feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(3, feeTransfers.size)
        val feeTransfer = feeTransfers.firstOrNull { it.toClientId == "Client6" }
        assertNotNull(feeTransfer)
        assertEquals("Client2", feeTransfer!!.fromClientId)
        assertEquals(7, operations.size)
    }

    @Test
    fun testMultipleFeeMoreThanOperationVolume() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.12))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.12))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)


        val feeInstructions = listOf(
                buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.3, targetClientId = "Client4")!!,
                buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 3.0, targetClientId = "Client5")!!,
                buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.5, targetClientId = "Client6")!!
        )
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)
    }

    @Test
    fun testMakerMultipleFeeMoreThanOperationVolume() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.12))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.12))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = listOf(
                buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE, makerSize = 0.3, targetClientId = "Client4")!!,
                buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE, makerSizeType = FeeSizeType.ABSOLUTE, makerSize = 3.0, targetClientId = "Client5")!!,
                buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE, makerSize = 0.5, targetClientId = "Client6")!!
        )
        assertFails { feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)
    }

    @Test
    fun testExternalMultipleFeeNotEnoughFundsAndMoreThanOperationVolume() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client3", "USD", 10.12))

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.12))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.12))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = listOf(
                buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.33, sourceClientId = "Client3", targetClientId = "Client4")!!,
                buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.33, sourceClientId = "Client3", targetClientId = "Client5")!!,
                buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.35, sourceClientId = "Client3", targetClientId = "Client6")!!
        )
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)
    }

    @Test
    fun testNoNegativeReceiptOperationAmount() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -900.0))
        val originalOperations = LinkedList(operations)
        val receiptOperation = operations.first()

        var feeInstructions = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 100.01, targetClientId = "Client4")!!)
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 50.0, targetClientId = "Client4")!!,
                buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 50.01, targetClientId = "Client4")!!)
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.PERCENTAGE, size = 0.12, targetClientId = "Client4")!!)
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.PERCENTAGE, size = 0.6, targetClientId = "Client4")!!,
                buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.PERCENTAGE, size = 0.6, targetClientId = "Client4")!!)
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)
    }

    @Test
    fun testNegativeReceiptOperationAmount() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -900.0))
        val receiptOperation = operations.first()

        val feeInstructions = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 100.0, targetClientId = "Client4")!!)
        val feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(1, feeTransfers.size)
        val feeTransfer = feeTransfers.first()
        assertEquals(100.0, feeTransfer.volume)
        assertEquals(2, operations.size)
        assertEquals(-1000.0, operations.firstOrNull { it.clientId == "Client1" }!!.amount)
        assertEquals(100.0, operations.firstOrNull { it.clientId == "Client4" }!!.amount)
    }

    @Test
    fun testNegativeReceiptOperationAmountMultipleFee() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -900.0))
        val receiptOperation = operations.first()

        val feeInstructions = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 50.0, targetClientId = "Client4")!!,
                buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 50.0, targetClientId = "Client4")!!)
        val feeTransfers = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(2, feeTransfers.size)
        assertEquals(50.0, feeTransfers[0].volume)
        assertEquals(50.0, feeTransfers[1].volume)
        assertEquals(3, operations.size)
        assertEquals(-1000.0, operations.firstOrNull { it.clientId == "Client1" }!!.amount)
        operations.filter { it.clientId == "Client4" }.forEach {
            assertEquals(50.0, it.amount)
        }
    }

}