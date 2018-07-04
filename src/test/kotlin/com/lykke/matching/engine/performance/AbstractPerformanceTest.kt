package com.lykke.matching.engine.performance

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
import com.lykke.matching.engine.database.TestMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.TestOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.TestPersistenceManager
import com.lykke.matching.engine.database.TestStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.notification.BalanceUpdateNotification
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.AbstractEvent
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.services.validators.impl.MarketOrderValidatorImpl
import com.lykke.matching.engine.services.validators.impl.MultiLimitOrderValidatorImpl
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractPerformanceTest {

    companion object {
        val REPEAT_TIMES = 100
    }

    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected lateinit var testSettingsDatabaseAccessor: TestConfigDatabaseAccessor
    protected lateinit var testConfigDatabaseAccessor: TestConfigDatabaseAccessor

    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService
    protected lateinit var genericLimitOrderService: GenericLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService


    protected lateinit var genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory
    protected lateinit var limitOrdersProcessorFactory: LimitOrdersProcessorFactory
    protected lateinit var genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory

    protected lateinit var assetsHolder: AssetsHolder
    protected lateinit var balancesHolder: BalancesHolder
    protected lateinit var assetsPairsHolder: AssetsPairsHolder
    protected lateinit var assetCache: AssetsCache
    protected lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder
    private lateinit var ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder
    private lateinit var stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder
    protected val testFileOrderDatabaseAccessor = ordersDatabaseAccessorsHolder.primaryAccessor as TestFileOrderDatabaseAccessor
    protected val stopOrderDatabaseAccessor = stopOrdersDatabaseAccessorsHolder.primaryAccessor as TestStopOrderBookDatabaseAccessor
    protected val testOrderDatabaseAccessor = TestOrderBookDatabaseAccessor(testFileOrderDatabaseAccessor)

    protected lateinit var assetPairsCache: AssetPairsCache
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache
    protected lateinit var persistenceManager: PersistenceManager

    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    val balanceUpdateQueue = LinkedBlockingQueue<BalanceUpdate>()

    val balanceUpdateNotificationQueue = LinkedBlockingQueue<BalanceUpdateNotification>()

    val clientLimitOrdersQueue  = LinkedBlockingQueue<LimitOrdersReport>()

    val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()

    val orderBookQueue = LinkedBlockingQueue<OrderBook>()

    val rabbitOrderBookQueue = LinkedBlockingQueue<OrderBook>()

    val rabbitSwapQueue  = LinkedBlockingQueue<MarketOrderWithTrades>()

    val trustedClientsLimitOrdersQueue  = LinkedBlockingQueue<LimitOrdersReport>()

    val quotesUpdateQueue = LinkedBlockingQueue<QuotesUpdate>()

    val tradeInfoQueue = LinkedBlockingQueue<TradeInfo>()

    val rabbitEventsQueue = LinkedBlockingQueue<AbstractEvent<*>>()

    val rabbitTrustedClientsEventsQueue = LinkedBlockingQueue<AbstractEvent<*>>()

    open fun initServices() {
        testSettingsDatabaseAccessor = TestConfigDatabaseAccessor()
        testSettingsDatabaseAccessor.addTrustedClient("Client3")

        testConfigDatabaseAccessor = TestConfigDatabaseAccessor()
        applicationSettingsCache = ApplicationSettingsCache(testConfigDatabaseAccessor, 60000)


        assetCache = AssetsCache(testBackOfficeDatabaseAccessor)
        assetsHolder = AssetsHolder(assetCache)
        balancesDatabaseAccessorsHolder = BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor(), null)
        persistenceManager = TestPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                ordersDatabaseAccessorsHolder,
                stopOrderDatabaseAccessor)
        balancesHolder = BalancesHolder(balancesDatabaseAccessorsHolder,
                persistenceManager,
                assetsHolder,
                balanceUpdateNotificationQueue, balanceUpdateQueue,
                applicationSettingsCache)

        testBalanceHolderWrapper = TestBalanceHolderWrapper(BalanceUpdateHandlerTest(balanceUpdateQueue, balanceUpdateNotificationQueue), balancesHolder)
        assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
        assetsPairsHolder = AssetsPairsHolder(assetPairsCache)




        genericLimitOrderService = GenericLimitOrderService(ordersDatabaseAccessorsHolder,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                quotesUpdateQueue, tradeInfoQueue,
                applicationSettingsCache)

        val messageSequenceNumberHolder = MessageSequenceNumberHolder(TestMessageSequenceNumberDatabaseAccessor())
        val notificationSender = MessageSender(rabbitEventsQueue, rabbitTrustedClientsEventsQueue)

        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrderDatabaseAccessor, genericLimitOrderService,
                persistenceManager)

        limitOrdersProcessorFactory = LimitOrdersProcessorFactory(assetsHolder, assetsPairsHolder, balancesHolder,
                genericLimitOrderService, clientLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue, trustedClientsLimitOrdersQueue, applicationSettingsCache,
                messageSequenceNumberHolder,
                notificationSender)

        genericLimitOrderProcessorFactory = GenericLimitOrderProcessorFactory(genericLimitOrderService,
                genericStopLimitOrderService,
                limitOrdersProcessorFactory,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                applicationSettingsCache, clientLimitOrdersQueue,
                messageSequenceNumberHolder,
                notificationSender)

        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)

        genericLimitOrdersCancellerFactory = GenericLimitOrdersCancellerFactory(testDictionariesDatabaseAccessor, assetsPairsHolder,
                balancesHolder, genericLimitOrderService, genericStopLimitOrderService,
                genericLimitOrderProcessorFactory, orderBookQueue, rabbitOrderBookQueue, clientLimitOrdersQueue, trustedClientsLimitOrdersQueue,
                messageSequenceNumberHolder,
                notificationSender)

        val multiLimitOrderValidatorImpl = MultiLimitOrderValidatorImpl(assetsHolder)
        multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService,
                genericLimitOrdersCancellerFactory,
                limitOrdersProcessorFactory,
                clientLimitOrdersQueue, trustedClientsLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                genericLimitOrderProcessorFactory,
                multiLimitOrderValidatorImpl,
                messageSequenceNumberHolder,
                notificationSender)

        val marketOrderValidator = MarketOrderValidatorImpl(assetsPairsHolder, assetsHolder, applicationSettingsCache)
        marketOrderService = MarketOrderService(
                genericLimitOrderService,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                clientLimitOrdersQueue, trustedClientsLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue,
                genericLimitOrderProcessorFactory, marketOrderValidator,
                messageSequenceNumberHolder,
                notificationSender)

    }
}