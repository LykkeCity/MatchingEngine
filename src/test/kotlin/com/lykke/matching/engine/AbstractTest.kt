package com.lykke.matching.engine

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.notification.*
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import org.junit.After
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import java.math.BigDecimal
import java.util.concurrent.BlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

abstract class AbstractTest {
    @Autowired
    lateinit var balancesHolder: BalancesHolder

    @Autowired
    protected lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    @Autowired
    protected lateinit var ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder

    @Autowired
    protected lateinit var stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder

    protected lateinit var testWalletDatabaseAccessor: TestWalletDatabaseAccessor
    protected lateinit var stopOrderDatabaseAccessor: TestStopOrderBookDatabaseAccessor

    @Autowired
    protected lateinit var testBackOfficeDatabaseAccessor: TestBackOfficeDatabaseAccessor

    @Autowired
    private lateinit var assetsCache: AssetsCache

    @Autowired
    protected lateinit var applicationSettingsHolder: ApplicationSettingsHolder

    @Autowired
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    protected lateinit var balanceUpdateHandlerTest: BalanceUpdateHandlerTest

    @Autowired
    protected lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @Autowired
    protected lateinit var assetPairsCache: AssetPairsCache

    @Autowired
    protected lateinit var persistenceManager: TestPersistenceManager

    @Autowired
    protected lateinit var testOrderDatabaseAccessor: TestFileOrderDatabaseAccessor

    @Autowired
    protected lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    protected lateinit var singleLimitOrderService: SingleLimitOrderService

    @Autowired
    protected lateinit var multiLimitOrderService: MultiLimitOrderService

    @Autowired
    protected lateinit var marketOrderService: MarketOrderService

    @Autowired
    protected lateinit var minVolumeOrderCanceller: MinVolumeOrderCanceller

    @Autowired
    protected lateinit var testTrustedClientsLimitOrderListener: TestTrustedClientsLimitOrderListener

    @Autowired
    protected lateinit var testClientLimitOrderListener: TestClientLimitOrderListener

    @Autowired
    protected lateinit var testLkkTradeListener: TestLkkTradeListener

    @Autowired
    protected lateinit var testOrderBookListener: TestOrderBookListener

    @Autowired
    protected lateinit var testRabbitOrderBookListener: TestRabbitOrderBookListener

    @Autowired
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    @Autowired
    protected lateinit var testOrderBookWrapper: TestOrderBookWrapper

    @Autowired
    protected lateinit var rabbitSwapListener: RabbitSwapListener

    @Autowired
    protected lateinit var tradesInfoListener: TradeInfoListener

    @Autowired
    protected lateinit var rabbitTransferQueue: BlockingQueue<CashTransferOperation>

    @Autowired
    protected lateinit var limitOrderCancelService: LimitOrderCancelService

    @Autowired
    protected lateinit var cashTransferOperationsService: CashTransferOperationService

    @Autowired
    protected lateinit var clientsEventsQueue: BlockingQueue<Event<*>>

    @Autowired
    protected lateinit var trustedClientsEventsQueue: BlockingQueue<ExecutionEvent>

    @Autowired
    @Qualifier("rabbitCashInOutQueue")
    protected lateinit var cashInOutQueue:  BlockingQueue<CashOperation>

    @Autowired
    protected lateinit var limitOrderMassCancelService: LimitOrderMassCancelService

    @Autowired
    protected lateinit var cashInOutOperationService: CashInOutOperationService

    @Autowired
    protected lateinit var multiLimitOrderCancelService: MultiLimitOrderCancelService

    protected open fun initServices() {
        testWalletDatabaseAccessor = balancesDatabaseAccessorsHolder.primaryAccessor as TestWalletDatabaseAccessor
        stopOrderDatabaseAccessor = stopOrdersDatabaseAccessorsHolder.primaryAccessor as TestStopOrderBookDatabaseAccessor
        clearMessageQueues()
        assetsCache.update()
        assetPairsCache.update()
        applicationSettingsCache.update()
        applicationSettingsHolder.update()
    }

    protected fun clearMessageQueues() {
        balanceUpdateHandlerTest.clear()
        tradesInfoListener.clear()

        testOrderBookListener.clear()
        testRabbitOrderBookListener.clear()

        testTrustedClientsLimitOrderListener.clear()
        testClientLimitOrderListener.clear()

        testLkkTradeListener.clear()
        rabbitSwapListener.clear()

        clientsEventsQueue.clear()
        trustedClientsEventsQueue.clear()
    }

    protected fun assertOrderBookSize(assetPairId: String, isBuySide: Boolean, size: Int) {
        assertEquals(size, testOrderDatabaseAccessor.getOrders(assetPairId, isBuySide).size)
        assertEquals(size, genericLimitOrderService.getOrderBook(assetPairId).getOrderBook(isBuySide).size)

        // check cache orders map size
        val allClientIds = testWalletDatabaseAccessor.loadWallets().keys
        assertEquals(size, allClientIds.sumBy { genericLimitOrderService.searchOrders(it, assetPairId, isBuySide).size })
    }

    protected fun assertStopOrderBookSize(assetPairId: String, isBuySide: Boolean, size: Int) {
        assertEquals(size, stopOrderDatabaseAccessor.getStopOrders(assetPairId, isBuySide).size)
        assertEquals(size, genericStopLimitOrderService.getOrderBook(assetPairId).getOrderBook(isBuySide).size)

        // check cache orders map size
        val allClientIds = testWalletDatabaseAccessor.loadWallets().keys
        assertEquals(size, allClientIds.sumBy { genericStopLimitOrderService.searchOrders(it, assetPairId, isBuySide).size })
    }

    protected fun assertBalance(clientId: String, assetId: String, balance: Double? = null, reserved: Double? = null) {
        if (balance != null) {
            assertEquals(BigDecimal.valueOf(balance), balancesHolder.getBalance(clientId, assetId))
            assertEquals(BigDecimal.valueOf(balance), testWalletDatabaseAccessor.getBalance(clientId, assetId))
        }
        if (reserved != null) {
            assertEquals(BigDecimal.valueOf(reserved), balancesHolder.getReservedBalance(clientId, assetId))
            assertEquals(BigDecimal.valueOf(reserved), testWalletDatabaseAccessor.getReservedBalance(clientId, assetId))
        }
    }

    @After
    open fun tearDown() {
        assertEqualsDbAndCacheLimitOrders()
        assertEqualsDbAndCacheStopLimitOrders()
        assertEqualsDbAndCacheBalances()
    }

    private fun assertEqualsDbAndCacheLimitOrders() {
        val primaryDbOrders = ordersDatabaseAccessorsHolder.primaryAccessor.loadLimitOrders()
        val secondaryDbOrders = ordersDatabaseAccessorsHolder.secondaryAccessor!!.loadLimitOrders()
        val cacheOrders = genericLimitOrderService.getAllOrderBooks().values.flatMap {
            val orders = mutableListOf<LimitOrder>()
            orders.addAll(it.getOrderBook(false))
            orders.addAll(it.getOrderBook(true))
            orders
        }
        assertEqualsOrderLists(primaryDbOrders, cacheOrders)
        assertEqualsOrderLists(secondaryDbOrders, cacheOrders)
    }

    private fun assertEqualsDbAndCacheStopLimitOrders() {
        val primaryDbOrders = stopOrdersDatabaseAccessorsHolder.primaryAccessor.loadStopLimitOrders()
        val secondaryDbOrders = stopOrdersDatabaseAccessorsHolder.secondaryAccessor!!.loadStopLimitOrders()
        val cacheOrders = genericStopLimitOrderService.getAllOrderBooks().values.flatMap {
            val orders = mutableListOf<LimitOrder>()
            orders.addAll(it.getOrderBook(false))
            orders.addAll(it.getOrderBook(true))
            orders
        }
        assertEqualsOrderLists(primaryDbOrders, cacheOrders)
        assertEqualsOrderLists(secondaryDbOrders, cacheOrders)
    }

    private fun assertEqualsOrderLists(orders1: Collection<LimitOrder>, orders2: Collection<LimitOrder>) {
        val ordersMap1 = orders1.groupBy { it.id }.mapValues { it.value.first() }
        val ordersMap2 = orders2.groupBy { it.id }.mapValues { it.value.first() }
        assertEquals(ordersMap1.size, ordersMap2.size)
        ordersMap1.forEach { id, order1 ->
            val order2 = ordersMap2[id]
            assertNotNull(order2)
            assertEqualsOrders(order1, order2!!)
        }
    }

    private fun assertEqualsOrders(order1: LimitOrder, order2: LimitOrder) {
        assertEquals(order1.id, order2.id)
        assertEquals(order1.externalId, order2.externalId)
        assertEquals(order1.status, order2.status)
        assertEquals(order1.statusDate, order2.statusDate)
        assertEquals(order1.remainingVolume, order2.remainingVolume)
        assertEquals(order1.lastMatchTime, order2.lastMatchTime)
        assertEquals(order1.reservedLimitVolume, order2.reservedLimitVolume)
        assertEquals(order1.price, order2.price)
    }

    private fun assertEqualsDbAndCacheBalances() {
        val primaryDbWallets = balancesDatabaseAccessorsHolder.primaryAccessor.loadWallets()
        val secondaryDbWallets = balancesDatabaseAccessorsHolder.secondaryAccessor?.loadWallets()
        val cacheWallets = balancesHolder.wallets.toMap()
        checkBalances(primaryDbWallets, cacheWallets)
        secondaryDbWallets?.let { checkBalances(it, cacheWallets) }
    }

    private fun checkBalances(wallets1: Map<String, Wallet>, wallets2: Map<String, Wallet>) {
        val balances1ByClientAndAsset = balancesByClientAndAsset(wallets1)
        val balances2ByClientAndAsset = balancesByClientAndAsset(wallets2)

        assertEquals(wallets1.size, wallets2.size)
        balances1ByClientAndAsset.forEach { id, assetBalance1 ->
            val assetBalance2 = balances2ByClientAndAsset[id] ?: throw Exception("Balances lists are different")
            assertEqualsBalances(assetBalance1, assetBalance2)
        }
    }

    private fun balancesByClientAndAsset(wallets: Map<String, Wallet>): Map<String, AssetBalance> {
        return wallets.values.flatMap { wallet ->
            wallet.balances.values.filter { assetBalance ->
                NumberUtils.equalsIgnoreScale(assetBalance.balance, BigDecimal.ZERO)
            }
        }.groupBy { assetBalance ->
            assetBalance.clientId + ";" + assetBalance.asset
        }.mapValues { it.value.single() }
    }

    private fun assertEqualsBalances(balance1: AssetBalance, balance2: AssetBalance) {
        assertEquals(balance1.asset, balance2.asset)
        assertEquals(balance1.clientId, balance2.clientId)
        assertEquals(balance1.balance.toDouble(), balance2.balance.toDouble())
        assertEquals(balance1.reserved.toDouble(), balance2.reserved.toDouble())
    }

    protected fun assertEventBalanceUpdate(clientId: String,
                                           assetId: String,
                                           oldBalance: String?,
                                           newBalance: String?,
                                           oldReserved: String?,
                                           newReserved: String?,
                                           balanceUpdates: Collection<BalanceUpdate>) {
        val balanceUpdate = balanceUpdates.single { it.walletId == clientId && it.assetId == assetId }
        assertEquals(oldBalance, balanceUpdate.oldBalance)
        assertEquals(newBalance, balanceUpdate.newBalance)
        assertEquals(oldReserved, balanceUpdate.oldReserved)
        assertEquals(newReserved, balanceUpdate.newReserved)
    }
}