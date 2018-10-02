package com.lykke.matching.engine.matching

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.CopyWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.services.GenericLimitOrderService
import org.apache.log4j.Logger
import org.junit.After
import org.junit.Assert.assertEquals
import com.lykke.matching.engine.utils.assertEquals
import org.junit.Before
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import java.math.BigDecimal
import java.util.Date
import java.util.concurrent.PriorityBlockingQueue
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

abstract class MatchingEngineTest {

    @Autowired
    protected lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @Autowired
    protected lateinit var assetsPairsHolder: AssetsPairsHolder

    protected lateinit var matchingEngine: MatchingEngine

    protected val now = Date()

    @Autowired
    protected lateinit var testDatabaseAccessor: TestFileOrderDatabaseAccessor

    @Autowired
    protected lateinit var testOrderBookWrapper: TestOrderBookWrapper

    @Autowired
    protected lateinit var genericService: GenericLimitOrderService

    @Autowired
    protected lateinit var balancesHolder: BalancesHolder

    @Autowired
    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var assetsHolder: AssetsHolder

    @Autowired
    protected lateinit var testBackOfficeDatabaseAccessor: TestBackOfficeDatabaseAccessor

    @Autowired
    private lateinit var feeProcessor: FeeProcessor

    @TestConfiguration
    open class Config {
        @Bean
        @Primary
        open fun testBackOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
            val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()

            testBackOfficeDatabaseAccessor.addAsset(Asset("USD", 2))
            testBackOfficeDatabaseAccessor.addAsset(Asset("EUR", 4))
            testBackOfficeDatabaseAccessor.addAsset(Asset("BTC", 8))
            testBackOfficeDatabaseAccessor.addAsset(Asset("CHF", 2))

            return testBackOfficeDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)

        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("BTCCHF", "BTC", "CHF", 3))

        initService()
    }

    @After
    fun tearDown() {
    }

    protected fun assertCompletedLimitOrders(completedLimitOrders: List<CopyWrapper<LimitOrder>>, checkOrderId: Boolean = true) {
        completedLimitOrders.map { it.origin!! }.forEach { completedOrder ->
            if (checkOrderId) {
                assertEquals("completed", completedOrder.id)
            }
            assertEquals(OrderStatus.Matched.name, completedOrder.status)
            assertEquals(BigDecimal.ZERO, completedOrder.remainingVolume)
            assertNotNull(completedOrder.reservedLimitVolume)
            assertEquals(BigDecimal.ZERO, completedOrder.reservedLimitVolume!!)
        }
    }

    protected fun assertMarketOrderMatchingResult(
            matchingResult: MatchingResult,
            marketBalance: BigDecimal? = null,
            marketPrice: BigDecimal? = null,
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
        matchingResult.apply()
        matchingEngine.apply()
        assertTrue { matchingResult.order is MarketOrder }
        assertEquals(marketPrice, matchingResult.order.takePrice())
        assertMatchingResult(matchingResult, marketBalance, status, skipSize, cancelledSize,
                lkkTradesSize, cashMovementsSize, marketOrderTradesSize, completedLimitOrdersSize, limitOrdersReportSize, orderBookSize)
    }

    protected fun assertLimitOrderMatchingResult(
            matchingResult: MatchingResult,
            remainingVolume: BigDecimal = BigDecimal.valueOf(100.0),
            marketBalance: BigDecimal? = BigDecimal.valueOf(1000.0),
            status: OrderStatus = OrderStatus.Processing,
            skipSize: Int = 0,
            cancelledSize: Int = 0,
            lkkTradesSize: Int = 0,
            cashMovementsSize: Int = 0,
            marketOrderTradesSize: Int = 0,
            completedLimitOrdersSize: Int = 0,
            limitOrdersReportSize: Int = 0,
            orderBookSize: Int = 0,
            matchedWithZeroLatestTrade: Boolean = false
    ) {
        matchingResult.apply()
        matchingEngine.apply()
        assertTrue { matchingResult.order is LimitOrder }
        val matchedOrder = matchingResult.order as LimitOrder
        assertEquals(remainingVolume, matchedOrder.remainingVolume)
        assertMatchingResult(matchingResult, marketBalance, status, skipSize, cancelledSize, lkkTradesSize,
                cashMovementsSize, marketOrderTradesSize, completedLimitOrdersSize, limitOrdersReportSize, orderBookSize, matchedWithZeroLatestTrade)
    }

    private fun assertMatchingResult(
            matchingResult: MatchingResult,
            marketBalance: BigDecimal? = BigDecimal.valueOf(1000.0),
            status: OrderStatus = OrderStatus.Processing,
            skipSize: Int = 0,
            cancelledSize: Int = 0,
            lkkTradesSize: Int = 0,
            cashMovementsSize: Int = 0,
            marketOrderTradesSize: Int = 0,
            completedLimitOrdersSize: Int = 0,
            limitOrdersReportSize: Int = 0,
            orderBookSize: Int = 0,
            matchedWithZeroLatestTrade: Boolean = false
    ) {
        assertEquals(status.name, matchingResult.order.status)
        if (marketBalance == null) {
            assertNull(matchingResult.marketBalance)
        } else {
            assertNotNull(matchingResult.marketBalance)
            assertEquals(marketBalance, matchingResult.marketBalance!!)
        }
        assertEquals(lkkTradesSize, matchingResult.lkkTrades.size)
        assertEquals(cancelledSize, matchingResult.cancelledLimitOrders.size)
        assertEquals(cashMovementsSize, matchingResult.ownCashMovements.size + matchingResult.oppositeCashMovements.size)
        assertEquals(marketOrderTradesSize, matchingResult.marketOrderTrades.size)
        assertEquals(completedLimitOrdersSize, matchingResult.completedLimitOrders.size)
        assertEquals(skipSize, matchingResult.skipLimitOrders.size)
        assertEquals(limitOrdersReportSize, matchingResult.limitOrdersReport?.orders?.size ?: 0)
        assertEquals(orderBookSize, matchingResult.orderBook.size)
        assertEquals(matchedWithZeroLatestTrade, matchingResult.matchedWithZeroLatestTrade)
    }

    private fun walletOperationTransform(operation: WalletOperation): WalletOperation =
            WalletOperation("", operation.externalId, operation.clientId, operation.assetId, now, operation.amount.stripTrailingZeros(), operation.reservedAmount.stripTrailingZeros(), operation.isFee)

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

    protected fun getOrderBook(assetPairId: String, isBuySide: Boolean): PriorityBlockingQueue<LimitOrder> =
            genericService.getOrderBook(assetPairId).getOrderBook(isBuySide)

    protected fun initService() {
        matchingEngine = MatchingEngine(Logger.getLogger(MatchingEngineTest::class.java.name), genericService, assetsHolder, assetsPairsHolder, balancesHolder, feeProcessor)
    }

}