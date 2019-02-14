package com.lykke.matching.engine.performance

import com.lykke.matching.engine.balance.WalletOperationsProcessorFactory
import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.TestUUIDHolder
import com.lykke.matching.engine.incoming.parsers.impl.LimitOrderCancelOperationContextParser
import com.lykke.matching.engine.incoming.parsers.impl.LimitOrderMassCancelOperationContextParser
import com.lykke.matching.engine.incoming.parsers.impl.MultilimitOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.impl.MultilimitOrderPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.ExecutionEventSender
import com.lykke.matching.engine.order.ExecutionPersistenceService
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.LimitOrderProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopLimitOrderProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.process.common.LimitOrdersCancellerImpl
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.transaction.CurrentTransactionBalancesHolderFactory
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.transaction.ExecutionEventsSequenceNumbersGenerator
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.business.impl.LimitOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.business.impl.StopOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.impl.MarketOrderValidatorImpl
import com.lykke.matching.engine.services.validators.input.impl.LimitOrderInputValidatorImpl
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.monitoring.OrderBookMidPriceChecker
import com.lykke.utils.logging.ThrottlingLogger
import org.mockito.Mockito
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.task.TaskExecutor
import java.util.Optional
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractPerformanceTest {

    companion object {
        val REPEAT_TIMES = 100
        private val LOGGER = ThrottlingLogger.getLogger(AbstractPerformanceTest::class.java.name)
    }

    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService
    protected lateinit var genericLimitOrderService: GenericLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService

    protected lateinit var assetsHolder: AssetsHolder
    protected lateinit var balancesHolder: BalancesHolder
    protected lateinit var balancesService: BalancesService
    protected lateinit var assetsPairsHolder: AssetsPairsHolder
    protected lateinit var assetCache: AssetsCache
    protected lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    protected val secondaryOrdersDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val primaryOrdersDatabaseAccessor = TestOrderBookDatabaseAccessor(secondaryOrdersDatabaseAccessor)
    private var ordersDatabaseAccessorsHolder = OrdersDatabaseAccessorsHolder(primaryOrdersDatabaseAccessor, secondaryOrdersDatabaseAccessor)

    protected val secondaryStopOrdersDatabaseAccessor = TestFileStopOrderDatabaseAccessor()
    protected val primaryStopOrdersDatabaseAccessor = TestStopOrderBookDatabaseAccessor(secondaryStopOrdersDatabaseAccessor)
    private val stopOrdersDatabaseAccessorsHolder = StopOrdersDatabaseAccessorsHolder(primaryStopOrdersDatabaseAccessor, secondaryStopOrdersDatabaseAccessor)
    private val priceDeviationThresholdHolder = PriceDeviationThresholdHolder(applicationSettingsHolder)

    protected lateinit var assetPairsCache: AssetPairsCache
    protected lateinit var applicationSettingsHolder: ApplicationSettingsHolder
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache
    protected lateinit var persistenceManager: PersistenceManager

    protected var rabbitEventsQueue = LinkedBlockingQueue<Event<*>>()
    protected var rabbitTrustedClientsEventsQueue = LinkedBlockingQueue<ExecutionEvent>()

    protected lateinit var singleLimitOrderContextParser: SingleLimitOrderContextParser
    protected lateinit var cashInOutContextParser: CashInOutContextParser
    protected lateinit var cashTransferContextParser: CashTransferContextParser
    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    private lateinit var feeProcessor: FeeProcessor
    private lateinit var expiryOrdersQueue: ExpiryOrdersQueue

    protected lateinit var messageBuilder: MessageBuilder

    val balanceUpdateQueue = LinkedBlockingQueue<BalanceUpdate>()

    val clientLimitOrdersQueue = LinkedBlockingQueue<LimitOrdersReport>()

    val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()

    val orderBookQueue = LinkedBlockingQueue<OrderBook>()

    val rabbitOrderBookQueue = LinkedBlockingQueue<OrderBook>()

    val rabbitEventDataQueue = LinkedBlockingQueue<ExecutionEventSender.RabbitEventData>()

    val rabbitSwapQueue  = LinkedBlockingQueue<MarketOrderWithTrades>()

    val trustedClientsLimitOrdersQueue = LinkedBlockingQueue<LimitOrdersReport>()

    val tradeInfoQueue = LinkedBlockingQueue<TradeInfo>()

    val currentTransactionBalancesHolderFactory = CurrentTransactionBalancesHolderFactory(balancesHolder)

    val walletOperationsProcessorFactory = WalletOperationsProcessorFactory(currentTransactionBalancesHolderFactory,
            applicationSettingsHolder, assetsHolder, balancesService)


    private var messageProcessingStatusHolder = Mockito.mock(MessageProcessingStatusHolder::class.java)

    private fun clearMessageQueues() {
        rabbitEventsQueue.clear()
        rabbitTrustedClientsEventsQueue.clear()
        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        lkkTradesQueue.clear()
        orderBookQueue.clear()
        rabbitOrderBookQueue.clear()
        rabbitSwapQueue.clear()
        trustedClientsLimitOrdersQueue.clear()
        tradeInfoQueue.clear()
    }

    open fun initServices() {
        val uuidHolder = TestUUIDHolder()
        clearMessageQueues()
        testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
        applicationSettingsCache = ApplicationSettingsCache(testSettingsDatabaseAccessor, ApplicationEventPublisher {})
        applicationSettingsHolder = ApplicationSettingsHolder(applicationSettingsCache)
        val limitOrdersCanceller = LimitOrdersCancellerImpl(applicationSettingsHolder)

        assetCache = AssetsCache(testBackOfficeDatabaseAccessor)
        assetsHolder = AssetsHolder(assetCache)
        balancesDatabaseAccessorsHolder = BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor(), null)
        persistenceManager = TestPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                ordersDatabaseAccessorsHolder,
                stopOrdersDatabaseAccessorsHolder)
        balancesHolder = BalancesHolder(balancesDatabaseAccessorsHolder)
        balancesService = BalancesServiceImpl(balancesHolder, persistenceManager, balanceUpdateQueue)

        testBalanceHolderWrapper = TestBalanceHolderWrapper(balancesService, balancesHolder)
        assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor, ApplicationEventPublisher {})
        assetsPairsHolder = AssetsPairsHolder(assetPairsCache)

        expiryOrdersQueue = ExpiryOrdersQueue()
        genericLimitOrderService = GenericLimitOrderService(ordersDatabaseAccessorsHolder,
                Optional.of(tradeInfoQueue),
                expiryOrdersQueue)

        feeProcessor = FeeProcessor(assetsHolder, assetsPairsHolder, genericLimitOrderService)

        val messageSequenceNumberHolder = MessageSequenceNumberHolder(TestMessageSequenceNumberDatabaseAccessor())
        val notificationSender = MessageSender(rabbitEventsQueue, rabbitTrustedClientsEventsQueue)
        val limitOrderInputValidator = LimitOrderInputValidatorImpl(applicationSettingsHolder)
        singleLimitOrderContextParser = SingleLimitOrderContextParser(assetsPairsHolder,
                assetsHolder,
                applicationSettingsHolder,
                uuidHolder,
                LOGGER)
        cashInOutContextParser = CashInOutContextParser(assetsHolder, uuidHolder)
        cashTransferContextParser = CashTransferContextParser(assetsHolder, uuidHolder)

        messageBuilder = MessageBuilder(SingleLimitOrderPreprocessor(singleLimitOrderContextParser, LinkedBlockingQueue<MessageWrapper>(), messageProcessingStatusHolder, ThrottlingLogger.getLogger("test")),
                cashInOutContextParser,
                cashTransferContextParser,
                LimitOrderCancelOperationContextParser(),
                LimitOrderMassCancelOperationContextParser(),
                MultilimitOrderPreprocessor(messageProcessingStatusHolder, limitOrderInputValidator, MultilimitOrderContextParser(ThrottlingLogger.getLogger("test"),
                        applicationSettingsHolder, assetsPairsHolder, assetsHolder, TestUUIDHolder()),
                        LinkedBlockingQueue<MessageWrapper>(), ThrottlingLogger.getLogger("test")))

        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder, expiryOrdersQueue)

        val executionEventsSequenceNumbersGenerator = ExecutionEventsSequenceNumbersGenerator(messageSequenceNumberHolder)
        val executionPersistenceService = ExecutionPersistenceService(persistenceManager)
        val executionEventSender = ExecutionEventSender(notificationSender,
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
                rabbitSwapQueue,
                lkkTradesQueue,
                genericLimitOrderService,
                orderBookQueue,
                rabbitOrderBookQueue,
                rabbitEventDataQueue,
                TaskExecutor { task -> task.run() })

        val executionDataApplyService = ExecutionDataApplyService(executionEventsSequenceNumbersGenerator,
                executionPersistenceService,
                executionEventSender)

        val midPriceHolder = MidPriceHolderImpl(1000, TestReadOnlyMidPriceDatabaseAccessor(), OrderBookMidPriceChecker())

        val executionContextFactory = ExecutionContextFactory(walletOperationsProcessorFactory,
                genericLimitOrderService,
                midPriceHolder,
                genericStopLimitOrderService,
                assetsHolder,
                priceDeviationThresholdHolder)

        val matchingResultHandlingHelper = MatchingResultHandlingHelper(applicationSettingsHolder)

        val matchingEngine = MatchingEngine(genericLimitOrderService, feeProcessor, uuidHolder)
        val priceDeviationThresholdHolder = PriceDeviationThresholdHolder(applicationSettingsHolder)

        val limitOrderProcessor = LimitOrderProcessor(
                LimitOrderBusinessValidatorImpl(),
                applicationSettingsHolder,
                matchingEngine,
                matchingResultHandlingHelper)

        val stopOrderProcessor = StopLimitOrderProcessor(
                StopOrderBusinessValidatorImpl(),
                applicationSettingsHolder,
                limitOrderProcessor,
                uuidHolder)

        val genericLimitOrdersProcessor = GenericLimitOrdersProcessor(limitOrderProcessor, stopOrderProcessor)

        val stopOrderBookProcessor = StopOrderBookProcessor(limitOrderProcessor, applicationSettingsHolder,
                executionContextFactory, priceDeviationThresholdHolder, midPriceHolder, uuidHolder)

        val previousLimitOrdersProcessor = PreviousLimitOrdersProcessor(genericLimitOrderService, genericStopLimitOrderService, limitOrdersCanceller)

        singleLimitOrderService = SingleLimitOrderService(executionContextFactory,
                genericLimitOrdersProcessor,
                stopOrderBookProcessor,
                executionDataApplyService,
                previousLimitOrdersProcessor,
                priceDeviationThresholdHolder,
                midPriceHolder,
                applicationSettingsHolder)

        multiLimitOrderService = MultiLimitOrderService(executionContextFactory,
                genericLimitOrdersProcessor,
                stopOrderBookProcessor,
                executionDataApplyService,
                previousLimitOrdersProcessor,
                balancesHolder,
                midPriceHolder,
                priceDeviationThresholdHolder)

        val marketOrderValidator = MarketOrderValidatorImpl(assetsPairsHolder, assetsHolder, applicationSettingsHolder)
        marketOrderService = MarketOrderService(matchingEngine,
                executionContextFactory,
                stopOrderBookProcessor,
                executionDataApplyService,
                matchingResultHandlingHelper,
                genericLimitOrderService,
                assetsPairsHolder,
                rabbitSwapQueue,
                marketOrderValidator,
                messageSequenceNumberHolder,
                priceDeviationThresholdHolder,
                midPriceHolder,
                messageProcessingStatusHolder,
                notificationSender,
                uuidHolder)

    }
}