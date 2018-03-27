package com.lykke.matching.engine.matching

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.buildWallet
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.apache.log4j.Logger
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.springframework.beans.factory.annotation.Autowired
import java.util.Date
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

abstract class MatchingEngineTest {

    protected val testDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val testWalletDatabaseAccessor = TestWalletDatabaseAccessor()
    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    protected val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    protected val assetsHolder = AssetsHolder(AssetsCache(testBackOfficeDatabaseAccessor))
    protected val assetsPairsHolder = AssetsPairsHolder(AssetPairsCache(testDictionariesDatabaseAccessor))
    protected val trustedClients = setOf<String>()

    protected lateinit var genericService: GenericLimitOrderService
    protected lateinit var matchingEngine: MatchingEngine
    protected val DELTA = 1e-9
    protected val now = Date()

    @Autowired
    protected lateinit var balancesHolder: BalancesHolder

    @Before
    fun setUp() {
        testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
        testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 4))
        testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
        testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3))

        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client1", "USD", 1000.0))
        testWalletDatabaseAccessor.insertOrUpdateWallet(buildWallet("Client2", "EUR", 1000.0))

        initService()
    }

    @After
    fun tearDown() {
    }

    protected fun assertCompletedLimitOrders(completedLimitOrders: List<NewLimitOrder>, checkOrderId: Boolean = true) {
        completedLimitOrders.forEach { completedOrder ->
            if (checkOrderId) {
                assertEquals("completed", completedOrder.id)
            }
            assertEquals(OrderStatus.Matched.name, completedOrder.status)
            assertEquals(0.0, completedOrder.remainingVolume, DELTA)
            assertNotNull(completedOrder.reservedLimitVolume)
            assertEquals(0.0, completedOrder.reservedLimitVolume!!, DELTA)
        }
    }

    protected fun assertMarketOrderMatchingResult(
            matchingResult: MatchingResult,
            marketBalance: Double? = null,
            marketPrice: Double? = null,
            status: OrderStatus = OrderStatus.NoLiquidity,
            skipSize: Int = 0,
            cancelledSize: Int = 0,
            lkkTradesSize: Int = 0,
            cashMovementsSize: Int = 0,
            marketOrderTradesSize: Int = 0,
            completedLimitOrdersSize: Int = 0,
            limitOrdersReportSize: Int = 0,
            orderBookSize: Int = 0
    ) {
        assertTrue { matchingResult.order is MarketOrder }
        assertEquals(marketPrice, matchingResult.order.takePrice())
        assertMatchingResult(matchingResult, marketBalance, status, skipSize, cancelledSize, lkkTradesSize, cashMovementsSize, marketOrderTradesSize, completedLimitOrdersSize, limitOrdersReportSize, orderBookSize)
    }

    protected fun assertLimitOrderMatchingResult(
            matchingResult: MatchingResult,
            remainingVolume: Double = 100.0,
            marketBalance: Double? = 1000.0,
            status: OrderStatus = OrderStatus.Processing,
            skipSize: Int = 0,
            cancelledSize: Int = 0,
            lkkTradesSize: Int = 0,
            cashMovementsSize: Int = 0,
            marketOrderTradesSize: Int = 0,
            completedLimitOrdersSize: Int = 0,
            limitOrdersReportSize: Int = 0,
            orderBookSize: Int = 0
    ) {
        assertTrue { matchingResult.order is NewLimitOrder }
        val matchedOrder = matchingResult.order as NewLimitOrder
        assertEquals(remainingVolume, matchedOrder.remainingVolume, DELTA)
        assertMatchingResult(matchingResult, marketBalance, status, skipSize, cancelledSize, lkkTradesSize, cashMovementsSize, marketOrderTradesSize, completedLimitOrdersSize, limitOrdersReportSize, orderBookSize)
    }

    private fun assertMatchingResult(
            matchingResult: MatchingResult,
            marketBalance: Double? = 1000.0,
            status: OrderStatus = OrderStatus.Processing,
            skipSize: Int = 0,
            cancelledSize: Int = 0,
            lkkTradesSize: Int = 0,
            cashMovementsSize: Int = 0,
            marketOrderTradesSize: Int = 0,
            completedLimitOrdersSize: Int = 0,
            limitOrdersReportSize: Int = 0,
            orderBookSize: Int = 0
    ) {
        assertEquals(status.name, matchingResult.order.status)
        if (marketBalance == null) {
            assertNull(matchingResult.marketBalance)
        } else {
            assertNotNull(matchingResult.marketBalance)
            assertEquals(marketBalance, matchingResult.marketBalance!!, DELTA)
        }
        assertEquals(lkkTradesSize, matchingResult.lkkTrades.size)
        assertEquals(cancelledSize, matchingResult.cancelledLimitOrders.size)
        assertEquals(cashMovementsSize, matchingResult.cashMovements.size)
        assertEquals(marketOrderTradesSize, matchingResult.marketOrderTrades.size)
        assertEquals(completedLimitOrdersSize, matchingResult.completedLimitOrders.size)
        assertEquals(skipSize, matchingResult.skipLimitOrders.size)
        assertEquals(limitOrdersReportSize, matchingResult.limitOrdersReport?.orders?.size ?: 0)
        assertEquals(orderBookSize, matchingResult.orderBook.size)
    }

    private fun walletOperationTransform(operation: WalletOperation): WalletOperation = WalletOperation("", operation.externalId, operation.clientId, operation.assetId, now, operation.amount, operation.reservedAmount, operation.isFee)

    protected fun assertCashMovementsEquals(expectedMovements: List<WalletOperation>, actualMovements: List<WalletOperation>) {
        assertEquals(expectedMovements.size, actualMovements.size)
        val expected = expectedMovements.map(this::walletOperationTransform)
        val actual = actualMovements.map(this::walletOperationTransform)
        assertTrue { expected.containsAll(actual) }
    }

    private fun lkkTradeTransform(trade: LkkTrade): LkkTrade = LkkTrade(trade.assetPair, trade.clientId, trade.price, trade.volume, now)

    protected fun assertLkkTradesEquals(expectedTrades: List<LkkTrade>, actualTrades: List<LkkTrade>) {
        assertEquals(expectedTrades.size, actualTrades.size)
        val expected = expectedTrades.map(this::lkkTradeTransform)
        val actual = actualTrades.map(this::lkkTradeTransform)
        assertTrue { expected.containsAll(actual) }
    }

    protected fun getOrderBook(assetPairId: String, isBuySide: Boolean): PriorityBlockingQueue<NewLimitOrder> =
            genericService.getOrderBook(assetPairId).getOrderBook(isBuySide)

    protected fun initService() {
        genericService = GenericLimitOrderService(testDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, trustedClients)
        matchingEngine = MatchingEngine(Logger.getLogger(MatchingEngineTest::class.java.name), genericService, assetsHolder, assetsPairsHolder, balancesHolder)
    }

}