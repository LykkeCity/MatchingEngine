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
import com.lykke.matching.engine.incoming.parsers.impl.*
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.order.ExecutionConfirmationService
import com.lykke.matching.engine.order.ExecutionEventSender
import com.lykke.matching.engine.order.ExecutionPersistenceService
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.*
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
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
        private val singleLimitOrderPreprocessorLogger = ThrottlingLogger.getLogger(SingleLimitOrderContextParser::class.java.name)
    }

    protected val testBackOfficeDatabaseAccessor = TestBackOfficeDatabaseAccessor()
    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService
    protected lateinit var genericLimitOrderService: GenericLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService

    protected lateinit var genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory

    protected lateinit var assetsHolder: AssetsHolder
    protected lateinit var balancesHolder: BalancesHolder
    protected lateinit var assetsPairsHolder: AssetsPairsHolder
    protected lateinit var assetCache: AssetsCache
    protected lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    protected val secondaryOrdersDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val primaryOrdersDatabaseAccessor = TestOrderBookDatabaseAccessor(secondaryOrdersDatabaseAccessor)
    private var ordersDatabaseAccessorsHolder = OrdersDatabaseAccessorsHolder(primaryOrdersDatabaseAccessor, secondaryOrdersDatabaseAccessor)

    protected val secondaryStopOrdersDatabaseAccessor = TestFileStopOrderDatabaseAccessor()
    protected val primaryStopOrdersDatabaseAccessor = TestStopOrderBookDatabaseAccessor(secondaryStopOrdersDatabaseAccessor)
    private var stopOrdersDatabaseAccessorsHolder = StopOrdersDatabaseAccessorsHolder(primaryStopOrdersDatabaseAccessor, secondaryStopOrdersDatabaseAccessor)

    protected lateinit var assetPairsCache: AssetPairsCache
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache
    protected lateinit var persistenceManager: PersistenceManager

    protected var rabbitEventsQueue = LinkedBlockingQueue<Event<*>>()
    protected var rabbitTrustedClientsEventsQueue = LinkedBlockingQueue<Event<*>>()

    protected lateinit var singleLimitOrderContextParser: SingleLimitOrderContextParser
    protected lateinit var cashInOutContextParser: CashInOutContextParser
    protected lateinit var cashTransferContextParser: CashTransferContextParser

    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    private lateinit var feeProcessor: FeeProcessor
    private lateinit var expiryOrdersQueue: ExpiryOrdersQueue

    protected lateinit var messageBuilder: MessageBuilder

    val balanceUpdateQueue = LinkedBlockingQueue<BalanceUpdate>()

    val clientLimitOrdersQueue  = LinkedBlockingQueue<LimitOrdersReport>()

    val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()

    val orderBookQueue = LinkedBlockingQueue<OrderBook>()

    val rabbitOrderBookQueue = LinkedBlockingQueue<OrderBook>()

    val rabbitSwapQueue  = LinkedBlockingQueue<MarketOrderWithTrades>()

    val trustedClientsLimitOrdersQueue  = LinkedBlockingQueue<LimitOrdersReport>()

    val tradeInfoQueue = LinkedBlockingQueue<TradeInfo>()

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
        clearMessageQueues()
        testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
        applicationSettingsCache = ApplicationSettingsCache(testSettingsDatabaseAccessor)

        assetCache = AssetsCache(testBackOfficeDatabaseAccessor)
        assetsHolder = AssetsHolder(assetCache)
        balancesDatabaseAccessorsHolder = BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor(), null)
        persistenceManager = TestPersistenceManager(balancesDatabaseAccessorsHolder.primaryAccessor,
                ordersDatabaseAccessorsHolder,
                stopOrdersDatabaseAccessorsHolder)
        balancesHolder = BalancesHolder(balancesDatabaseAccessorsHolder,
                persistenceManager, assetsHolder,
                balanceUpdateQueue, applicationSettingsCache)

        testBalanceHolderWrapper = TestBalanceHolderWrapper(BalanceUpdateHandlerTest(balanceUpdateQueue), balancesHolder)
        assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
        assetsPairsHolder = AssetsPairsHolder(assetPairsCache)

        expiryOrdersQueue = ExpiryOrdersQueue()
        genericLimitOrderService = GenericLimitOrderService(ordersDatabaseAccessorsHolder,
                tradeInfoQueue,
                expiryOrdersQueue)

        feeProcessor = FeeProcessor(assetsHolder, assetsPairsHolder, genericLimitOrderService)

        val messageSequenceNumberHolder = MessageSequenceNumberHolder(TestMessageSequenceNumberDatabaseAccessor())
        val notificationSender = MessageSender(rabbitEventsQueue, rabbitTrustedClientsEventsQueue)
        val limitOrderInputValidator = LimitOrderInputValidatorImpl(applicationSettingsCache)
        singleLimitOrderContextParser = SingleLimitOrderContextParser(assetsPairsHolder, assetsHolder, applicationSettingsCache, singleLimitOrderPreprocessorLogger)
        cashInOutContextParser = CashInOutContextParser(assetsHolder)
        cashTransferContextParser = CashTransferContextParser(assetsHolder)

        messageBuilder = MessageBuilder(singleLimitOrderContextParser,
                cashInOutContextParser,
                cashTransferContextParser,
                LimitOrderCancelOperationContextParser(),
                LimitOrderMassCancelOperationContextParser())

        genericStopLimitOrderService = GenericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder,
                expiryOrdersQueue)

        val executionPersistenceService = ExecutionPersistenceService(persistenceManager)
        val executionEventSender = ExecutionEventSender(messageSequenceNumberHolder,
                notificationSender,
                clientLimitOrdersQueue,
                trustedClientsLimitOrdersQueue,
                rabbitSwapQueue,
                lkkTradesQueue,
                genericLimitOrderService,
                orderBookQueue,
                rabbitOrderBookQueue)
        val executionConfirmationService = ExecutionConfirmationService(executionPersistenceService, executionEventSender)

        val midPriceHolder = MidPriceHolder(1000, TestReadOnlyMidPriceDatabaseAccessor(), assetsPairsHolder)

        val executionContextFactory = ExecutionContextFactory(balancesHolder,
                genericLimitOrderService,
                genericStopLimitOrderService,
                midPriceHolder,
                assetsHolder)

        val matchingResultHandlingHelper = MatchingResultHandlingHelper(applicationSettingsCache)

        val matchingEngine = MatchingEngine(genericLimitOrderService, feeProcessor)
        val priceDeviationThresholdHolder = PriceDeviationThresholdHolder(assetPairsCache, applicationSettingsCache)

        val limitOrderProcessor = LimitOrderProcessor(limitOrderInputValidator,
                LimitOrderBusinessValidatorImpl(),
                applicationSettingsCache,
                matchingEngine,
                midPriceHolder,
                priceDeviationThresholdHolder,
                matchingResultHandlingHelper)

        val stopOrderProcessor = StopLimitOrderProcessor(limitOrderInputValidator,
                StopOrderBusinessValidatorImpl(),
                applicationSettingsCache,
                limitOrderProcessor)

        val genericLimitOrdersProcessor = GenericLimitOrdersProcessor(limitOrderProcessor, stopOrderProcessor)

        val stopOrderBookProcessor = StopOrderBookProcessor(limitOrderProcessor, applicationSettingsCache)

        genericLimitOrdersCancellerFactory = GenericLimitOrdersCancellerFactory(executionContextFactory,
                stopOrderBookProcessor,
                executionConfirmationService,
                testDictionariesDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                genericLimitOrderService,
                genericStopLimitOrderService)

        val previousLimitOrdersProcessor = PreviousLimitOrdersProcessor(genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory)

        singleLimitOrderService = SingleLimitOrderService(executionContextFactory,
                genericLimitOrdersProcessor,
                stopOrderBookProcessor,
                executionConfirmationService,
                previousLimitOrdersProcessor)

        multiLimitOrderService = MultiLimitOrderService(executionContextFactory,
                genericLimitOrdersProcessor,
                stopOrderBookProcessor,
                executionConfirmationService,
                previousLimitOrdersProcessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                applicationSettingsCache)

        val marketOrderValidator = MarketOrderValidatorImpl(limitOrderInputValidator, assetsPairsHolder, assetsHolder, applicationSettingsCache)
        marketOrderService = MarketOrderService(matchingEngine,
                executionContextFactory,
                stopOrderBookProcessor,
                executionConfirmationService,
                matchingResultHandlingHelper,
                genericLimitOrderService,
                assetsPairsHolder,
                rabbitSwapQueue,
                marketOrderValidator,
                messageSequenceNumberHolder,
                priceDeviationThresholdHolder,
                midPriceHolder,
                notificationSender)

    }
}