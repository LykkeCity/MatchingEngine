package com.lykke.matching.engine.performance

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.TestBackOfficeDatabaseAccessor
import com.lykke.matching.engine.database.TestConfigDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestFileOrderDatabaseAccessor
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
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.notification.QuotesUpdate
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.JsonSerializable
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.SingleLimitOrderService
import com.lykke.matching.engine.utils.config.RedisConfig
import com.lykke.matching.engine.services.validators.impl.MarketOrderValidatorImpl
import com.lykke.matching.engine.services.validators.impl.MultiLimitOrderValidatorImpl
import org.mockito.Mockito
import org.springframework.context.ApplicationEventPublisher
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractPerformanceTest {

    companion object {
        val REPEAT_TIMES = 100
    }

    protected var testOrderDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected lateinit var testSettingsDatabaseAccessor: TestConfigDatabaseAccessor
    protected lateinit var stopOrderDatabaseAccessor: TestStopOrderBookDatabaseAccessor
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

    protected lateinit var assetPairsCache: AssetPairsCache
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache
    protected val applicationEventPublicher = Mockito.mock(ApplicationEventPublisher::class.java)
    protected lateinit var persistenceManager: PersistenceManager

    protected lateinit var tradesInfoQueue: LinkedBlockingQueue<TradeInfo>
    protected lateinit var quotesNotificationQueue: LinkedBlockingQueue<QuotesUpdate>
    protected lateinit var clientsLimitOrdersQueue: LinkedBlockingQueue<JsonSerializable>
    protected lateinit var trustedClientsLimitOrdersQueue: LinkedBlockingQueue<JsonSerializable>
    protected lateinit var lkkTradesQueue: LinkedBlockingQueue<List<LkkTrade>>
    protected lateinit var orderBookQueue: LinkedBlockingQueue<OrderBook>
    protected lateinit var rabbitOrderBookQueue: LinkedBlockingQueue<JsonSerializable>
    protected lateinit var rabbitSwapQueue: LinkedBlockingQueue<JsonSerializable>
    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper


    open fun initServices() {
        testOrderDatabaseAccessor = TestFileOrderDatabaseAccessor()

        testSettingsDatabaseAccessor = TestConfigDatabaseAccessor()
        testSettingsDatabaseAccessor.addTrustedClient("Client3")

        testConfigDatabaseAccessor = TestConfigDatabaseAccessor()
        applicationSettingsCache = ApplicationSettingsCache(testConfigDatabaseAccessor, 60000)


        assetCache = AssetsCache(testBackOfficeDatabaseAccessor)
        assetsHolder = AssetsHolder(assetCache)
        balancesDatabaseAccessorsHolder = BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor(), null,
                RedisConfig("", 0,0,false, null, 0, 1, 2))
        persistenceManager = TestPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor)
        balancesHolder = BalancesHolder(balancesDatabaseAccessorsHolder,
                persistenceManager,
                assetsHolder,
                applicationEventPublicher,
                applicationSettingsCache)

        testBalanceHolderWrapper = TestBalanceHolderWrapper(BalanceUpdateHandlerTest(), balancesHolder)
        assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
        assetsPairsHolder = AssetsPairsHolder(assetPairsCache)


        tradesInfoQueue = LinkedBlockingQueue()
        quotesNotificationQueue = LinkedBlockingQueue()

        genericLimitOrderService = GenericLimitOrderService(testOrderDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                tradesInfoQueue,
                quotesNotificationQueue,
                applicationSettingsCache)

        clientsLimitOrdersQueue = LinkedBlockingQueue()

        stopOrderDatabaseAccessor = TestStopOrderBookDatabaseAccessor()
        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrderDatabaseAccessor, genericLimitOrderService)

        trustedClientsLimitOrdersQueue = LinkedBlockingQueue()
        lkkTradesQueue = LinkedBlockingQueue()
        orderBookQueue = LinkedBlockingQueue()
        rabbitOrderBookQueue = LinkedBlockingQueue()
        limitOrdersProcessorFactory = LimitOrdersProcessorFactory(assetsHolder, assetsPairsHolder, balancesHolder,
                genericLimitOrderService, applicationSettingsCache,
                trustedClientsLimitOrdersQueue, clientsLimitOrdersQueue,
                lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue)

        genericLimitOrderProcessorFactory = GenericLimitOrderProcessorFactory(genericLimitOrderService,
                genericStopLimitOrderService,
                limitOrdersProcessorFactory,
                clientsLimitOrdersQueue,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                applicationSettingsCache)

        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)

        genericLimitOrdersCancellerFactory = GenericLimitOrdersCancellerFactory(testDictionariesDatabaseAccessor, assetsPairsHolder,
                balancesHolder, genericLimitOrderService, genericStopLimitOrderService,
                genericLimitOrderProcessorFactory, trustedClientsLimitOrdersQueue,
                clientsLimitOrdersQueue, orderBookQueue, rabbitOrderBookQueue)

        val multiLimitOrderValidatorImpl = MultiLimitOrderValidatorImpl(assetsHolder)
        multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService,
                genericLimitOrdersCancellerFactory,
                limitOrdersProcessorFactory,
                trustedClientsLimitOrdersQueue,
                clientsLimitOrdersQueue,
                orderBookQueue,
                rabbitOrderBookQueue,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                lkkTradesQueue,
                genericLimitOrderProcessorFactory, multiLimitOrderValidatorImpl)

        rabbitSwapQueue = LinkedBlockingQueue()
        val marketOrderValidator = MarketOrderValidatorImpl(assetsPairsHolder, assetsHolder, applicationSettingsCache)
        marketOrderService = MarketOrderService(testBackOfficeDatabaseAccessor,
                genericLimitOrderService,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                trustedClientsLimitOrdersQueue,
                clientsLimitOrdersQueue,
                orderBookQueue,
                rabbitOrderBookQueue,
                rabbitSwapQueue,
                lkkTradesQueue,
                genericLimitOrderProcessorFactory, marketOrderValidator)

    }
}