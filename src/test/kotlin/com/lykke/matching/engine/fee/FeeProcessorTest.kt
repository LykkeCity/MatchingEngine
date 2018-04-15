package com.lykke.matching.engine.fee

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.services.GenericLimitOrderService
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
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FeeProcessorTest {

    private val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val testOrderBookDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor))
    private val assetsPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
    private val assetsPairsHolder = AssetsPairsHolder(assetsPairsCache)
    private lateinit var balancesHolder: BalancesHolder
    private lateinit var feeProcessor: FeeProcessor
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    private val applicationSettingsCache: ApplicationSettingsCache = ApplicationSettingsCache(TestSettingsDatabaseAccessor())

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "USD", 1000.0))

        initServices()
    }

    private fun initServices() {
        assetsPairsCache.update()
        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, applicationSettingsCache)
        genericLimitOrderService = GenericLimitOrderService(testOrderBookDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, LinkedBlockingQueue(), LinkedBlockingQueue(), applicationSettingsCache)
        feeProcessor = FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)
    }

    @Test
    fun testNoPercentageFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 10.0))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        initServices()

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.0))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.0))
        val originalOperations = LinkedList(operations)
        val receiptOperation = operations[1]

        var feeInstructions = buildFeeInstructions()
        var fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(0, fees.size)
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.NO_FEE)
        fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(1, fees.size)
        assertNull(fees.first().transfer)
        assertEquals(originalOperations, operations)

//        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE)
//        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
//        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.05)
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 1.01, targetClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

//        feeInstructions = buildFeeInstructions(type = FeeType.EXTERNAL_FEE)
//        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
//        assertEquals(originalOperations, operations)

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

        feeInstructions = buildLimitOrderFeeInstructions(type = FeeType.CLIENT_FEE, makerSize = 0.02, targetClientId = "Client3", makerFeeModificator = 0.0)
        assertFails { feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations, 0.01) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildLimitOrderFeeInstructions(type = FeeType.CLIENT_FEE, makerSize = 0.02, targetClientId = "Client3", makerFeeModificator = -10.0)
        assertFails { feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations, 0.01) }
        assertEquals(originalOperations, operations)

        feeInstructions = buildLimitOrderFeeInstructions(type = FeeType.CLIENT_FEE, makerSize = 0.02, targetClientId = "Client3", makerFeeModificator = 50.0)
        assertFails { feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations, -0.01) }
        assertEquals(originalOperations, operations)

        // Negative fee size
        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = -0.01, targetClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        // Empty order book for asset pair to convert to fee asset
        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.1, targetClientId = "Client3", assetIds = listOf("EUR"))
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)
    }

    @Test
    fun testNoAbsoluteFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 0.09))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        initServices()

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -0.5))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 0.5))
        val originalOperations = LinkedList(operations)
        val receiptOperation = operations[1]

        var feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.6, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        // test not enough funds for another asset fee
        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.1, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3", assetIds = listOf("EUR"))
        assertFailsWith(NotEnoughFundsFeeException::class) { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)

        // Negative fee size
        feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = -0.1, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3")
        assertFails { feeProcessor.processFee(feeInstructions, receiptOperation, operations) }
        assertEquals(originalOperations, operations)
    }

    @Test
    fun testAbsoluteFeeCashout() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        initServices()

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -0.5))
        val receiptOperation = operations[0]

        val feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.4, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3")

        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(1, fees.size)
        assertEquals(0.4, fees.first().transfer!!.volume)
        assertEquals("USD", fees.first().transfer!!.asset)
        assertEquals(2, operations.size)
        assertEquals(-0.5, operations.first.amount)
    }

    @Test
    fun testPercentFeeCashout() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        initServices()

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -0.5))
        val receiptOperation = operations[0]

        val feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.4, sizeType = FeeSizeType.PERCENTAGE, targetClientId = "Client3")

        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(1, fees.size)
        assertEquals(0.2, fees.first().transfer!!.volume)
        assertEquals("USD", fees.first().transfer!!.asset)
        assertEquals(2, operations.size)
        assertEquals(-0.5, operations.first.amount)
    }

    @Test
    fun testAnotherAssetFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 0.6543, 0.0))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 4))
        initServices()

        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -0.5))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 0.5))
        val receiptOperation = operations[1]

        val feeInstructions = buildFeeInstructions(type = FeeType.CLIENT_FEE, size = 0.6543, sizeType = FeeSizeType.ABSOLUTE, targetClientId = "Client3", assetIds = listOf("EUR"))
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(1, fees.size)
        assertEquals(0.6543, fees.first().transfer!!.volume)
        assertEquals("EUR", fees.first().transfer!!.asset)
        assertEquals(4, operations.size)
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
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client3", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.11, fee.transfer!!.volume)

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
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client3", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(1.1, fee.transfer!!.volume)

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
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client3", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.01, fee.transfer!!.volume)

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
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client3", fee.transfer!!.fromClientId)
        assertEquals("Client4", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.11, fee.transfer!!.volume)

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
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client4", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.11, fee.transfer!!.volume)

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
        val fees = feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client3", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.21, fee.transfer!!.volume)

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

        val fees = feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations)

        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client3", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.2, fee.transfer!!.volume)

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
        initServices()

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
        val fees = feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations)

        assertEquals(3, fees.size)
        var fee = fees[0]
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client3", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.21, fee.transfer!!.volume)

        fee = fees[1]
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client5", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.41, fee.transfer!!.volume)

        fee = fees[2]
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client4", fee.transfer!!.fromClientId)
        assertEquals("Client3", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.31, fee.transfer!!.volume)


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
        initServices()

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
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)

        assertEquals(3, fees.size)
        val fee = fees.firstOrNull { it.transfer?.toClientId == "Client6" }
        assertNotNull(fee)
        assertEquals("Client2", fee!!.transfer!!.fromClientId)
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
    fun testNegativeReceiptOperationAmount() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -900.0))
        val receiptOperation = operations.first()

        val feeInstructions = listOf(buildFeeInstruction(type = FeeType.CLIENT_FEE, sizeType = FeeSizeType.ABSOLUTE, size = 100.0, targetClientId = "Client4")!!)
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals(100.0, fee.transfer!!.volume)
        assertEquals(2, operations.size)
        assertEquals(-900.0, operations.firstOrNull { it.clientId == "Client1" }!!.amount)
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
        val fees = feeProcessor.processFee(feeInstructions, receiptOperation, operations)
        assertEquals(2, fees.size)
        assertEquals(50.0, fees[0].transfer!!.volume)
        assertEquals(50.0, fees[1].transfer!!.volume)
        assertEquals(3, operations.size)
        assertEquals(-900.0, operations.firstOrNull { it.clientId == "Client1" }!!.amount)

        val feeOperations = operations.filter { it.isFee }
        assertEquals(2, feeOperations.size)
        assertEquals("Client4", feeOperations[0].clientId)
        assertEquals(50.0, feeOperations[0].amount)
        assertEquals("Client4", feeOperations[1].clientId)
        assertEquals(50.0, feeOperations[1].amount)

    }

    @Test
    fun testMakerFeeModificator() {
        val operations = LinkedList<WalletOperation>()
        val now = Date()
        operations.add(WalletOperation("1", null, "Client1", "USD", now, -10.1))
        operations.add(WalletOperation("2", null, "Client2", "USD", now, 10.1))
        val receiptOperation = operations[1]
        val originalOperations = LinkedList(operations)

        val feeInstructions = buildLimitOrderFeeInstructions(type = FeeType.CLIENT_FEE, takerSize = 0.01, makerSize = 0.02, targetClientId = "Client3", makerFeeModificator = 50.0)
        val fees = feeProcessor.processMakerFee(feeInstructions, receiptOperation, operations, 0.01)

        assertEquals(1, fees.size)
        val fee = fees.first()
        assertEquals("USD", fee.transfer!!.asset)
        assertEquals("Client2", fee.transfer!!.fromClientId)
        assertEquals("Client3", fee.transfer!!.toClientId)
        assertNull(fee.transfer!!.externalId)
        assertEquals(0.393469340287, fee.transfer!!.feeCoef) // 1 - exp(-0.01*50)
        assertEquals(now, fee.transfer!!.dateTime)
        assertEquals(0.08, fee.transfer!!.volume)

        assertEquals(3, operations.size)
        assertEquals(originalOperations[0], operations[0])
        assertFalse { operations[0].isFee }
        assertEquals(10.02, operations[1].amount)
        assertFalse { operations[1].isFee }
        assertEquals(0.08, operations[2].amount)
        assertEquals("Client3", operations[2].clientId)
        assertTrue { operations[2].isFee }
    }
}