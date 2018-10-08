package com.lykke.matching.engine.performance

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.incoming.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.*
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.business.impl.LimitOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.business.impl.StopOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.impl.MarketOrderValidatorImpl
import com.lykke.matching.engine.services.validators.input.impl.LimitOrderInputValidatorImpl
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractPerformanceTest {

    companion object {
        val REPEAT_TIMES = 100
    }

    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

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
    protected val testOrderDatabaseAccessor = ordersDatabaseAccessorsHolder.primaryAccessor as TestFileOrderDatabaseAccessor
    protected val stopOrderDatabaseAccessor = stopOrdersDatabaseAccessorsHolder.primaryAccessor as TestStopOrderBookDatabaseAccessor

    protected lateinit var assetPairsCache: AssetPairsCache
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache
    protected lateinit var persistenceManager: PersistenceManager

    protected lateinit var rabbitEventsQueue: LinkedBlockingQueue<Event<*>>
    protected lateinit var rabbitTrustedClientsEventsQueue: LinkedBlockingQueue<Event<*>>
    protected val messageBuilder = MessageBuilder(CashInOutContextParser(assetsHolder),
            CashTransferContextParser(assetsHolder), limitOrderCancelOperationContextParser,
            limitOrderCancelMassOperationContextParser, singleLimitOrderContextParser)


    protected lateinit var singleLimitOrderContextParser: ContextParser<SingleLimitOrderParsedData>
    protected lateinit var limitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData>
    protected lateinit var limitOrderCancelMassOperationContextParser: ContextParser<LimitOrderMassCancelOperationParsedData>

    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    private lateinit var feeProcessor: FeeProcessor
    private lateinit var expiryOrdersQueue: ExpiryOrdersQueue

    val balanceUpdateQueue = LinkedBlockingQueue<BalanceUpdate>()

    val clientLimitOrdersQueue  = LinkedBlockingQueue<LimitOrdersReport>()

    val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()

    val orderBookQueue = LinkedBlockingQueue<OrderBook>()

    val rabbitOrderBookQueue = LinkedBlockingQueue<OrderBook>()

    val rabbitSwapQueue  = LinkedBlockingQueue<MarketOrderWithTrades>()

    val trustedClientsLimitOrdersQueue  = LinkedBlockingQueue<LimitOrdersReport>()

    val tradeInfoQueue = LinkedBlockingQueue<TradeInfo>()


    open fun initServices() {
        testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
        applicationSettingsCache = ApplicationSettingsCache(testSettingsDatabaseAccessor)

        assetCache = AssetsCache(testBackOfficeDatabaseAccessor)
        assetsHolder = AssetsHolder(assetCache)
        balancesDatabaseAccessorsHolder = BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor(), null)
        persistenceManager = TestPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                ordersDatabaseAccessorsHolder,
                stopOrderDatabaseAccessor)
        balancesHolder = BalancesHolder(balancesDatabaseAccessorsHolder,
                persistenceManager, assetsHolder,
                balanceUpdateQueue, applicationSettingsCache)

        testBalanceHolderWrapper = TestBalanceHolderWrapper(BalanceUpdateHandlerTest(balanceUpdateQueue), balancesHolder)
        assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
        assetsPairsHolder = AssetsPairsHolder(assetPairsCache)

        feeProcessor = FeeProcessor(assetsHolder, assetsPairsHolder, genericLimitOrderService)
        expiryOrdersQueue = ExpiryOrdersQueue()

        genericLimitOrderService = GenericLimitOrderService(ordersDatabaseAccessorsHolder,
                assetsPairsHolder,
                balancesHolder,
                tradeInfoQueue,
                applicationSettingsCache,
                expiryOrdersQueue)

        val messageSequenceNumberHolder = MessageSequenceNumberHolder(TestMessageSequenceNumberDatabaseAccessor())
        val notificationSender = MessageSender(rabbitEventsQueue, rabbitTrustedClientsEventsQueue)
        val limitOrderInputValidator = LimitOrderInputValidatorImpl(applicationSettingsCache)
        val singleLimitOrderPreprocessorLogger = ThrottlingLogger.getLogger(SingleLimitOrderContextParser::class.java.name)
        singleLimitOrderContextParser = SingleLimitOrderContextParser(assetsPairsHolder, assetsHolder, applicationSettingsCache, singleLimitOrderPreprocessorLogger)
        limitOrderCancelOperationContextParser = LimitOrderCancelOperationContextParser()
        limitOrderCancelMassOperationContextParser = LimitOrderMassCancelOperationContextParser()
        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder, genericLimitOrderService,
                persistenceManager, expiryOrdersQueue)

        limitOrdersProcessorFactory = LimitOrdersProcessorFactory(balancesHolder, LimitOrderBusinessValidatorImpl(), limitOrderInputValidator,
                genericLimitOrderService, clientLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue,
                trustedClientsLimitOrdersQueue, messageSequenceNumberHolder, notificationSender, applicationSettingsCache)

        genericLimitOrderProcessorFactory = GenericLimitOrderProcessorFactory(genericLimitOrderService,
                genericStopLimitOrderService,
                limitOrdersProcessorFactory,
                StopOrderBusinessValidatorImpl(), assetsHolder, assetsPairsHolder,
                balancesHolder, clientLimitOrdersQueue, feeProcessor, SingleLimitOrderContextParser(assetsPairsHolder, assetsHolder, applicationSettingsCache, singleLimitOrderPreprocessorLogger),
                messageSequenceNumberHolder, notificationSender)

        singleLimitOrderService = SingleLimitOrderService(genericLimitOrderProcessorFactory)

        genericLimitOrdersCancellerFactory = GenericLimitOrdersCancellerFactory(testDictionariesDatabaseAccessor, assetsHolder, assetsPairsHolder,
                balancesHolder, genericLimitOrderService, genericStopLimitOrderService,
                genericLimitOrderProcessorFactory, orderBookQueue, rabbitOrderBookQueue, clientLimitOrdersQueue, trustedClientsLimitOrdersQueue, messageSequenceNumberHolder, notificationSender)

        multiLimitOrderService = MultiLimitOrderService(genericLimitOrderService,
                genericLimitOrdersCancellerFactory,
                limitOrdersProcessorFactory,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                genericLimitOrderProcessorFactory,
                feeProcessor,
                applicationSettingsCache)

        val marketOrderValidator = MarketOrderValidatorImpl(limitOrderInputValidator, assetsPairsHolder, assetsHolder, applicationSettingsCache)
        marketOrderService = MarketOrderService(
                genericLimitOrderService,
                genericLimitOrdersCancellerFactory,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                clientLimitOrdersQueue, trustedClientsLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue,
                genericLimitOrderProcessorFactory, marketOrderValidator,  feeProcessor, applicationSettingsCache, messageSequenceNumberHolder, notificationSender)

    }
}