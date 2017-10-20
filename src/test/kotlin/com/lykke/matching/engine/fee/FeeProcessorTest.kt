package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeInstruction
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
import org.junit.Before
import org.junit.Test
import java.util.Date
import java.util.LinkedList
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class FeeProcessorTest {

    private var testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    private lateinit var balancesHolder: BalancesHolder
    private lateinit var feeProcessor: FeeProcessor

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2, "USD"))
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
    }

    @Test
    fun testClientFee() {
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
        assertEquals(9.99, operations[1].amount)
        assertEquals(0.11, operations[2].amount)
        assertEquals("Client3", operations[2].clientId)

    }

    @Test
    fun testExternalFee() {
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
        assertEquals(0.11, operations[3].amount)
        assertEquals("Client4", operations[3].clientId)
    }

    @Test
    fun testExternalFeeNotEnoughFunds() {
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
        assertEquals(0.11, operations[2].amount)
        assertEquals("Client4", operations[2].clientId)
    }

    private fun buildFeeInstruction(type: FeeType? = null, size: Double? = null, sourceClientId: String? = null, targetClientId: String? = null): FeeInstruction? {
        return if (type == null) null
        else return FeeInstruction(type, size, sourceClientId, targetClientId)
    }
}