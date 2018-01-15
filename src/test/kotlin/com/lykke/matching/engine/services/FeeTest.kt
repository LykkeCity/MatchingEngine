package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import org.apache.log4j.Logger
import org.junit.Before
import org.junit.Test
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals

class FeeTest {
    private var testDatabaseAccessor = TestFileOrderDatabaseAccessor()
    private val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    private var testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    private val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    private val balanceUpdateQueue = LinkedBlockingQueue<JsonSerializable>()
    private val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    private val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    private val trustedClientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    private val clientLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    private val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    private val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    private val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor, 60000))
    private val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testWalletDatabaseAccessor, 60000))
    private val trustedClients = setOf<String>()
    private val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()
    private lateinit var balancesHolder: BalancesHolder
    private lateinit var genericService: GenericLimitOrderService
    private lateinit var matchingEngine: MatchingEngine
    private lateinit var singleLimitOrderService: SingleLimitOrderService
    private lateinit var multiLimitOrderService: MultiLimitOrderService
    private lateinit var marketOrderService: MarketOrderService

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 2))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))

        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testWalletDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))

        initServices()
    }

    private fun initServices() {
        balancesHolder = BalancesHolder(testWalletDatabaseAccessor, assetsHolder, LinkedBlockingQueue<BalanceUpdateNotification>(), balanceUpdateQueue, trustedClients)
        genericService = GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients)
        matchingEngine = MatchingEngine(Logger.getLogger(FeeTest::class.java.name), genericService, assetsHolder, assetsPairsHolder, balancesHolder)
        singleLimitOrderService = SingleLimitOrderService(genericService, trustedClientsLimitOrdersQueue, clientLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, lkkTradesQueue)
        multiLimitOrderService = MultiLimitOrderService(genericService, trustedClientsLimitOrdersQueue, clientLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, emptySet(), balancesHolder, lkkTradesQueue)
        marketOrderService = MarketOrderService(testBackOfficeDatabaseAccessor, genericService, assetsHolder, assetsPairsHolder, balancesHolder, trustedClientsLimitOrdersQueue, clientLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue, lkkTradesQueue)
    }

    @Test
    fun testBuyLimitOrderFeeAnotherAsset() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "BTC", balance = 0.1))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "USD", balance = 100.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client4", assetId = "USD", balance = 10.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client4", assetId = "BTC", balance = 0.1))

        testDatabaseAccessor.addLimitOrder(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = -0.05,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = 0.04,
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!,
                        buildLimitOrderFeeInstruction(
                                type = FeeType.EXTERNAL_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = 0.05,
                                sourceClientId = "Client4",
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!
                )
        ))
        initServices()

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = 0.005,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = 0.03,
                                targetClientId = "Client3",
                                assetIds = listOf("USD"))!!,
                        buildLimitOrderFeeInstruction(
                                type = FeeType.EXTERNAL_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = 0.02,
                                sourceClientId = "Client4",
                                targetClientId = "Client3",
                                assetIds = listOf("USD"))!!
                )
        )))

        assertEquals(75.0, balancesHolder.getBalance("Client1", "USD"))
        assertEquals(0.0948, balancesHolder.getBalance("Client1", "BTC"))
        assertEquals(0.00045, balancesHolder.getBalance("Client3", "BTC"))
        assertEquals(3.75, balancesHolder.getBalance("Client3", "USD"))
        assertEquals(22.75, balancesHolder.getBalance("Client2", "USD"))
        assertEquals(0.005, balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(0.09975, balancesHolder.getBalance("Client4", "BTC"))
        assertEquals(8.5, balancesHolder.getBalance("Client4", "USD"))
    }

    @Test
    fun testSellMarketOrderFeeAnotherAsset() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 100.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "BTC", balance = 0.1))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client4", assetId = "USD", balance = 10.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client4", assetId = "BTC", balance = 0.1))

        testDatabaseAccessor.addLimitOrder(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", price = 15154.123, volume = 0.005412,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = 0.04,
                                targetClientId = "Client3",
                                assetIds = listOf("USD"))!!,
                        buildLimitOrderFeeInstruction(
                                type = FeeType.EXTERNAL_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = 0.05,
                                sourceClientId = "Client4",
                                targetClientId = "Client3",
                                assetIds = listOf("USD"))!!
                )
        ))
        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(
                clientId = "Client2", assetId = "BTCUSD", volume = -0.005,
                fees = listOf(
                        buildLimitOrderFeeInstruction(
                                type = FeeType.CLIENT_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = 0.03,
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!,
                        buildLimitOrderFeeInstruction(
                                type = FeeType.EXTERNAL_FEE,
                                takerSizeType = FeeSizeType.PERCENTAGE,
                                takerSize = 0.02,
                                sourceClientId = "Client4",
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!
                )
        )))

        assertEquals(0.005, balancesHolder.getBalance("Client1", "BTC"))
        assertEquals(21.19, balancesHolder.getBalance("Client1", "USD"))
        assertEquals(75.77, balancesHolder.getBalance("Client2", "USD"))
        assertEquals(0.09484954, balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(6.83, balancesHolder.getBalance("Client3", "USD"))
        assertEquals(0.00025077, balancesHolder.getBalance("Client3", "BTC"))
        assertEquals(0.09989969, balancesHolder.getBalance("Client4", "BTC"))
        assertEquals(6.21, balancesHolder.getBalance("Client4", "USD"))
    }

    @Test
    fun testOrderBookNotEnoughFundsForFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 750.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "BTC", balance = 0.0503))

        for (i in 1..5) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    uid = "order$i", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01,
                    fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                            makerSizeType = FeeSizeType.PERCENTAGE,
                            makerSize = 0.01,
                            targetClientId = "Client3",
                            assetIds = listOf("BTC"))!!))))
        }

        assertEquals(5, testDatabaseAccessor.getOrders("BTCUSD", false).size)

        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.05
        )))

        val result = clientLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders.first { it.order.externalId == "order" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order1" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order2" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order3" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order4" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order5" }.order.status)
        assertEquals(0.02, balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(0, testDatabaseAccessor.getOrders("BTCUSD", false).size)
        assertEquals(1, testDatabaseAccessor.getOrders("BTCUSD", true).size)
    }

    @Test
    fun testOrderBookNotEnoughFundsForMultipleFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 600.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "BTC", balance = 0.0403))

        for (i in 1..2) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    uid = "order$i", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01,
                    fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                            makerSizeType = FeeSizeType.PERCENTAGE,
                            makerSize = 0.01,
                            targetClientId = "Client3",
                            assetIds = listOf("BTC"))!!))))
        }

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order3", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01,
                fees = listOf(
                        buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = 0.01,
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!,
                        buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                                makerSizeType = FeeSizeType.PERCENTAGE,
                                makerSize = 0.01,
                                targetClientId = "Client3",
                                assetIds = listOf("BTC"))!!))))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order4", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01,
                fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                        makerSizeType = FeeSizeType.PERCENTAGE,
                        makerSize = 0.01,
                        targetClientId = "Client3",
                        assetIds = listOf("BTC"))!!))))

        assertEquals(4, testDatabaseAccessor.getOrders("BTCUSD", false).size)

        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.04
        )))

        val result = clientLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Processing.name, result.orders.first { it.order.externalId == "order" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order1" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order2" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order3" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order4" }.order.status)
        assertEquals(0.01, balancesHolder.getBalance("Client2", "BTC"))
        assertEquals(0, testDatabaseAccessor.getOrders("BTCUSD", false).size)
        assertEquals(1, testDatabaseAccessor.getOrders("BTCUSD", true).size)
    }

    @Test
    fun testMarketNotEnoughFundsForFee1() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 764.99))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "BTC", balance = 0.05))

        for (i in 1..5) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01
            )))
        }

        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.05,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = 0.02,
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        val result = clientLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.NotEnoughFunds.name, result.orders.first { it.order.externalId == "order" }.order.status)
        assertEquals(0, balanceUpdateQueue.size)
        assertEquals(0, testDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(5, testDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testMarketNotEnoughFundsForFee2() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 764.99))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "BTC", balance = 0.05))

        for (i in 1..5) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01
            )))
        }

        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = 0.05,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = 0.02,
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        val result = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.NotEnoughFunds.name, result.order.status)
        assertEquals(0, balanceUpdateQueue.size)
        assertEquals(0, testDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(5, testDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testMarketNotEnoughFundsForFee3() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 764.99))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "BTC", balance = 0.05))

        for (i in 1..5) {
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.01
            )))
        }

        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(
                clientId = "Client1", assetId = "BTCUSD", volume = -750.0, straight = false,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = 0.02,
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        val result = rabbitSwapQueue.poll() as MarketOrderWithTrades
        assertEquals(OrderStatus.NotEnoughFunds.name, result.order.status)
        assertEquals(0, balanceUpdateQueue.size)
        assertEquals(0, testDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(5, testDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testNotEnoughFundsForFee() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 151.5))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client2", assetId = "BTC", balance = 0.01521))

        val feeSizes = arrayListOf(0.01, 0.1, 0.01)
        feeSizes.forEachIndexed { index, feeSize ->
            singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                    uid = "order$index", clientId = "Client2", assetId = "BTCUSD", price = 15000.0, volume = -0.005,
                    fees = listOf(buildLimitOrderFeeInstruction(type = FeeType.CLIENT_FEE,
                            makerSizeType = FeeSizeType.PERCENTAGE,
                            makerSize = feeSize,
                            targetClientId = "Client3",
                            assetIds = listOf("BTC"))!!))))
        }

        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order4", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.01,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = 0.02,
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        var result = clientLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.NotEnoughFunds.name, result.orders.first { it.order.externalId == "order4" }.order.status)
        assertEquals(0, balanceUpdateQueue.size)
        assertEquals(0, testDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(3, testDatabaseAccessor.getOrders("BTCUSD", false).size)

        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                uid = "order5", clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.01,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = 0.01,
                        targetClientId = "Client3",
                        assetIds = listOf("USD"))!!))))

        result = clientLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order0" }.order.status)
        assertEquals(OrderStatus.Cancelled.name, result.orders.first { it.order.externalId == "order1" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order2" }.order.status)
        assertEquals(OrderStatus.Matched.name, result.orders.first { it.order.externalId == "order5" }.order.status)
        assertEquals(0, testDatabaseAccessor.getOrders("BTCUSD", true).size)
        assertEquals(0, testDatabaseAccessor.getOrders("BTCUSD", false).size)
    }

    @Test
    fun testIllegalFeeAsset() {
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet(clientId = "Client1", assetId = "USD", balance = 200.0))

        singleLimitOrderService.processMessage(buildLimitOrderWrapper(buildLimitOrder(
                clientId = "Client1", assetId = "BTCUSD", price = 15000.0, volume = 0.01,
                fees = listOf(buildLimitOrderFeeInstruction(
                        type = FeeType.CLIENT_FEE,
                        takerSizeType = FeeSizeType.PERCENTAGE,
                        takerSize = 0.01,
                        targetClientId = "Client3",
                        assetIds = listOf("EUR"))!!))))

        val result = clientLimitOrdersQueue.poll() as LimitOrdersReport
        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.InvalidFee.name, result.orders.first().order.status)
    }

    private fun buildLimitOrderFeeInstruction(type: FeeType? = null,
                                              takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                              takerSize: Double? = null,
                                              makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
                                              makerSize: Double? = null,
                                              sourceClientId: String? = null,
                                              targetClientId: String? = null,
                                              assetIds: List<String> = listOf()): NewLimitOrderFeeInstruction? {
        return if (type == null) null
        else return NewLimitOrderFeeInstruction(type, takerSizeType, takerSize, makerSizeType, makerSize, sourceClientId, targetClientId, assetIds)
    }
}