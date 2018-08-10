package com.lykke.matching.engine

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.notification.*
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.CashOperation
import com.lykke.matching.engine.outgoing.messages.CashTransferOperation
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.business.CashInOutOperationBusinessValidator
import com.lykke.matching.engine.services.validators.business.CashTransferOperationBusinessValidator
import org.springframework.beans.factory.annotation.Autowired
import java.math.BigDecimal
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import org.springframework.beans.factory.annotation.Qualifier
import java.util.concurrent.BlockingQueue

abstract class AbstractTest {
    @Autowired
    lateinit var balancesHolder: BalancesHolder

    @Autowired
    protected lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    protected lateinit var testWalletDatabaseAccessor: TestWalletDatabaseAccessor

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
    private lateinit var cashInOutOperationBusinessValidator: CashInOutOperationBusinessValidator

    @Autowired
    private lateinit var cashTransferOperationBusinessValidator: CashTransferOperationBusinessValidator

    @Autowired
    protected lateinit var reservedCashInOutOperationService: ReservedCashInOutOperationService

    @Autowired
    protected lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @Autowired
    protected lateinit var assetsPairsHolder: AssetsPairsHolder

    @Autowired
    protected lateinit var assetPairsCache: AssetPairsCache

    @Autowired
    protected lateinit var balanceUpdateService: BalanceUpdateService

    @Autowired
    protected lateinit var persistenceManager: TestPersistenceManager

    @Autowired
    protected lateinit var messageSequenceNumberHolder: MessageSequenceNumberHolder

    @Autowired
    protected lateinit var messageSender: MessageSender

    @Autowired
    protected lateinit var clientsEventsQueue: BlockingQueue<Event<*>>

    @Autowired
    protected lateinit var trustedClientsEventsQueue: BlockingQueue<ExecutionEvent>

    @Autowired
    protected lateinit var testOrderDatabaseAccessor: TestFileOrderDatabaseAccessor

    @Autowired
    private lateinit var genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory

    @Autowired
    protected lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    protected lateinit var multiLimitOrderService: MultiLimitOrderService

    @Autowired
    protected lateinit var marketOrderService: MarketOrderService

    @Autowired
    protected lateinit var minVolumeOrderCanceller: MinVolumeOrderCanceller

    @Autowired
    protected lateinit var genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory

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
    protected lateinit var stopOrderDatabaseAccessor: TestStopOrderBookDatabaseAccessor

    @Autowired
    protected lateinit var rabbitSwapListener: RabbitSwapListener

    @Autowired
    protected lateinit var tradesInfoListener: TradeInfoListener

    @Autowired
    protected lateinit var rabbitTransferQueue: BlockingQueue<CashTransferOperation>

    @Autowired
    protected lateinit var cashTransferOperationsService: CashTransferOperationService

    protected val quotesNotificationQueue = LinkedBlockingQueue<QuotesUpdate>()

    @Autowired
    @Qualifier("rabbitCashInOutQueue")
    protected lateinit var cashInOutQueue:  BlockingQueue<CashOperation>

    @Autowired
    protected lateinit var cashInOutOperationService: CashInOutOperationService

    protected lateinit var singleLimitOrderService: SingleLimitOrderService

    protected lateinit var reservedBalanceUpdateService: ReservedBalanceUpdateService
    protected lateinit var limitOrderCancelService: LimitOrderCancelService
    protected lateinit var limitOrderMassCancelService: LimitOrderMassCancelService
    protected lateinit var multiLimitOrderCancelService: MultiLimitOrderCancelService

    protected open fun initServices() {
        testWalletDatabaseAccessor = balancesDatabaseAccessorsHolder.primaryAccessor as TestWalletDatabaseAccessor
        clearMessageQueues()
        assetsCache.update()
        assetPairsCache.update()
        applicationSettingsCache.update()

        reservedBalanceUpdateService = ReservedBalanceUpdateService(balancesHolder)
        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)

        limitOrderCancelService = LimitOrderCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)
        multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory)
        limitOrderMassCancelService = LimitOrderMassCancelService(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)
        multiLimitOrderCancelService = MultiLimitOrderCancelService(genericLimitOrderService, genericLimitOrdersCancellerFactory)
    }

    protected fun clearMessageQueues() {
        quotesNotificationQueue.clear()

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