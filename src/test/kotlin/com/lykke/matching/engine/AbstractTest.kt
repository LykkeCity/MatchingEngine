package com.lykke.matching.engine

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.CashInOutOperationValidator
import com.lykke.matching.engine.services.validators.CashTransferOperationValidator
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.services.validators.MultiLimitOrderValidator
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import org.junit.After
import org.springframework.beans.factory.annotation.Autowired
import java.math.BigDecimal
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import com.lykke.matching.engine.utils.assertEquals
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
    protected lateinit var testOrderDatabaseAccessor: TestOrderBookDatabaseAccessor
    protected lateinit var stopOrderDatabaseAccessor: TestStopOrderBookDatabaseAccessor

    @Autowired
    protected lateinit var testBackOfficeDatabaseAccessor: TestBackOfficeDatabaseAccessor

    @Autowired
    private lateinit var assetsCache: AssetsCache

    @Autowired
    protected lateinit var assetsHolder: AssetsHolder

    @Autowired
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    protected lateinit var balanceUpdateHandlerTest: BalanceUpdateHandlerTest

    @Autowired
    private lateinit var cashInOutOperationValidator: CashInOutOperationValidator

    @Autowired
    private lateinit var cashTransferOperationValidator: CashTransferOperationValidator

    @Autowired
    protected lateinit var reservedCashInOutOperationService: ReservedCashInOutOperationService

    @Autowired
    protected lateinit var marketOrderValidator: MarketOrderValidator

    @Autowired
    protected lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @Autowired
    protected lateinit var assetsPairsHolder: AssetsPairsHolder

    @Autowired
    protected lateinit var assetPairsCache: AssetPairsCache

    @Autowired
    private lateinit var multiLimitOrderValidator: MultiLimitOrderValidator

    @Autowired
    protected lateinit var balanceUpdateService: BalanceUpdateService

    @Autowired
    protected lateinit var persistenceManager: TestPersistenceManager

    protected val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()
    protected val tradesInfoQueue = LinkedBlockingQueue<TradeInfo>()
    protected val orderBookQueue = LinkedBlockingQueue<OrderBook>()
    protected val rabbitOrderBookQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val trustedClientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val clientsLimitOrdersQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()
    protected val dbTransferOperationQueue = LinkedBlockingQueue<TransferOperation>()
    protected val rabbitSwapQueue = LinkedBlockingQueue<JsonSerializable>()
    protected val cashInOutQueue = LinkedBlockingQueue<JsonSerializable>()

    protected val rabbitTransferQueue = LinkedBlockingQueue<JsonSerializable>()

    protected lateinit var feeProcessor: FeeProcessor

    protected lateinit var genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory
    protected lateinit var limitOrdersProcessorFactory: LimitOrdersProcessorFactory
    protected lateinit var minVolumeOrderCanceller: MinVolumeOrderCanceller

    protected lateinit var genericLimitOrderService: GenericLimitOrderService
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    protected lateinit var cashInOutOperationService: CashInOutOperationService
    protected lateinit var cashTransferOperationsService: CashTransferOperationService
    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService

    protected lateinit var reservedBalanceUpdateService: ReservedBalanceUpdateService
    protected lateinit var limitOrderCancelService: LimitOrderCancelService
    protected lateinit var limitOrderMassCancelService: LimitOrderMassCancelService
    protected lateinit var multiLimitOrderCancelService: MultiLimitOrderCancelService

    private var initialized = false
    protected open fun initServices() {
        initialized = true
        testWalletDatabaseAccessor = balancesDatabaseAccessorsHolder.primaryAccessor as TestWalletDatabaseAccessor
        testOrderDatabaseAccessor = ordersDatabaseAccessorsHolder.primaryAccessor as TestOrderBookDatabaseAccessor
        stopOrderDatabaseAccessor = stopOrdersDatabaseAccessorsHolder.primaryAccessor as TestStopOrderBookDatabaseAccessor
        clearMessageQueues()
        assetsCache.update()
        assetPairsCache.update()
        applicationSettingsCache.update()

        genericLimitOrderService = GenericLimitOrderService(testOrderDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, tradesInfoQueue, quotesNotificationQueue, applicationSettingsCache)
        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrderDatabaseAccessor, genericLimitOrderService, persistenceManager)

        feeProcessor = FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)
        limitOrdersProcessorFactory = LimitOrdersProcessorFactory(assetsHolder, assetsPairsHolder, balancesHolder, genericLimitOrderService,
                applicationSettingsCache, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue)

        val genericLimitOrderProcessorFactory = GenericLimitOrderProcessorFactory(genericLimitOrderService, genericStopLimitOrderService,
                limitOrdersProcessorFactory, clientsLimitOrdersQueue, assetsHolder, assetsPairsHolder, balancesHolder, applicationSettingsCache)

        genericLimitOrdersCancellerFactory = GenericLimitOrdersCancellerFactory(testDictionariesDatabaseAccessor, assetsPairsHolder,
                balancesHolder, genericLimitOrderService, genericStopLimitOrderService,
                genericLimitOrderProcessorFactory, trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue)

        cashTransferOperationsService = CashTransferOperationService(balancesHolder, assetsHolder, rabbitTransferQueue,
                dbTransferOperationQueue,
                FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService),
                cashTransferOperationValidator)

        minVolumeOrderCanceller = MinVolumeOrderCanceller(testDictionariesDatabaseAccessor, assetsPairsHolder, genericLimitOrderService, genericLimitOrdersCancellerFactory)
        reservedBalanceUpdateService = ReservedBalanceUpdateService(balancesHolder)
        cashInOutOperationService = CashInOutOperationService(assetsHolder, balancesHolder, cashInOutQueue, feeProcessor,cashInOutOperationValidator)
        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)
        multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService, genericLimitOrdersCancellerFactory, limitOrdersProcessorFactory, trustedClientsLimitOrdersQueue,
                clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, assetsHolder, assetsPairsHolder, balancesHolder, lkkTradesQueue, genericLimitOrderProcessorFactory, multiLimitOrderValidator)
        marketOrderService = MarketOrderService(testBackOfficeDatabaseAccessor, genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder,
                trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue,
                lkkTradesQueue, genericLimitOrderProcessorFactory, marketOrderValidator)
        limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory, persistenceManager)
        multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory)
        limitOrderMassCancelService = LimitOrderMassCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)
        multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory)
    }

    protected fun clearMessageQueues() {
        balanceUpdateHandlerTest.clear()
        quotesNotificationQueue.clear()
        tradesInfoQueue.clear()
        orderBookQueue.clear()
        rabbitOrderBookQueue.clear()
        trustedClientsLimitOrdersQueue.clear()
        clientsLimitOrdersQueue.clear()
        lkkTradesQueue.clear()
        rabbitSwapQueue.clear()
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
        if (initialized) {
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
        assertEquals(order1.remainingVolume, order2.remainingVolume)
        assertEquals(order1.lastMatchTime, order2.lastMatchTime)
        assertEquals(order1.reservedLimitVolume, order2.reservedLimitVolume)
    }
}