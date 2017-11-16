package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrderFeeInstruction
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.LinkedList
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
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
    fun testNoFee() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.0))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.0))
        val originalOperations = LinkedList(operations)
        val receiptOperation = operations[1]

        var feeInstruction = buildFeeInstruction()
        var feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction = buildFeeInstruction(type = FeeType.NO_FEE)
        feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction = buildFeeInstruction(type = FeeType.CLIENT_FEE)
        feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction = buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.05)
        feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction = buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.0, targetClientId = "Client3")
        feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction = buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 1.01, targetClientId = "Client3")
        feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction = buildFeeInstruction(type = FeeType.EXTERNAL_FEE)
        feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction = buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.05, sourceClientId = "Client3")
        feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction = buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.05, targetClientId = "Client4")
        feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
        assertEquals(originalOperations, operations)

        feeInstruction =  buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.01, targetClientId = "Client3")
        feeTransfer = feeProcessor.processMakerFee(feeInstruction, receiptOperation, operations)
        assertNull(feeTransfer)
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

        val feeInstruction = buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.01, targetClientId = "Client3")
        val feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)

        assertNotNull(feeTransfer)
        assertEquals("USD", feeTransfer!!.asset)
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
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstruction = buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 0.1, targetClientId = "Client3")
        val feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)

        assertNotNull(feeTransfer)
        assertEquals("USD", feeTransfer!!.asset)
        assertEquals("Client2", feeTransfer.fromClientId)
        assertEquals("Client3", feeTransfer.toClientId)
        assertNull(feeTransfer.externalId)
        assertEquals(now, feeTransfer.dateTime)
        assertEquals(0.1, feeTransfer.volume)

        assertEquals(3, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertFalse { operations[0].isFee }
        assertEquals(10.0, operations[1].amount)
        assertFalse { operations[1].isFee }
        assertEquals(0.1, operations[2].amount)
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

        val feeInstruction = buildFeeInstruction(type = FeeType.CLIENT_FEE, size = 0.0001, targetClientId = "Client3")
        val feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)

        assertNotNull(feeTransfer)
        assertEquals("USD", feeTransfer!!.asset)
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

        val feeInstruction = buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.01, sourceClientId = "Client3", targetClientId = "Client4")
        val feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)

        assertNotNull(feeTransfer)
        assertEquals("USD", feeTransfer!!.asset)
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

        val feeInstruction = buildFeeInstruction(type = FeeType.EXTERNAL_FEE, size = 0.01, sourceClientId = "Client3", targetClientId = "Client4")
        val feeTransfer = feeProcessor.processFee(feeInstruction, receiptOperation, operations)

        assertNotNull(feeTransfer)
        assertEquals("USD", feeTransfer!!.asset)
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

        val feeInstruction = buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE, takerSize = 0.01, makerSize = 0.02, targetClientId = "Client3")
        val feeTransfer = feeProcessor.processMakerFee(feeInstruction, receiptOperation, operations)

        assertNotNull(feeTransfer)
        assertEquals("USD", feeTransfer!!.asset)
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

        val feeInstruction = buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                takerSize = 0.1,
                makerSizeType = FeeSizeType.ABSOLUTE,
                makerSize = 0.2,
                targetClientId = "Client3")

        val feeTransfer = feeProcessor.processMakerFee(feeInstruction, receiptOperation, operations)

        assertNotNull(feeTransfer)
        assertEquals("USD", feeTransfer!!.asset)
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

    private fun buildFeeInstruction(type: FeeType? = null,
                                    sizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                    size: Double? = null,
                                    sourceClientId: String? = null,
                                    targetClientId: String? = null): FeeInstruction? {
        return if (type == null) null
        else return FeeInstruction(type, sizeType, size, sourceClientId, targetClientId)
    }

    private fun buildLimitOrderFeeInstruction(type: FeeType? = null,
                                              takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                              takerSize: Double? = null,
                                              makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                              makerSize: Double? = null,
                                              sourceClientId: String? = null,
                                              targetClientId: String? = null): FeeInstruction? {
        return if (type == null) null
        else return LimitOrderFeeInstruction(type, takerSizeType, takerSize, makerSizeType, makerSize, sourceClientId, targetClientId)
    }
}