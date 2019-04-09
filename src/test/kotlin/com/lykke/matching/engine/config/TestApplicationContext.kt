package com.lykke.matching.engine.config

import com.lykke.client.accounts.ClientAccountsCache
import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.spring.JsonConfig
import com.lykke.matching.engine.config.spring.QueueConfig
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.incoming.MessageRouter
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.impl.*
import com.lykke.matching.engine.incoming.preprocessor.impl.CashInOutPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.CashTransferPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageWrapper
import com.lykke.matching.engine.notification.*
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutor
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.*
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.services.CashInOutOperationService
import com.lykke.matching.engine.services.CashTransferOperationService
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MarketOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.MultiLimitOrderService
import com.lykke.matching.engine.services.ReservedCashInOutOperationService
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.services.validators.ReservedCashInOutOperationValidator
import com.lykke.matching.engine.services.validators.business.*
import com.lykke.matching.engine.services.validators.business.impl.*
import com.lykke.matching.engine.services.validators.impl.MarketOrderValidatorImpl
import com.lykke.matching.engine.services.validators.impl.ReservedCashInOutOperationValidatorImpl
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.services.validators.input.CashInOutOperationInputValidator
import com.lykke.matching.engine.services.validators.input.CashTransferOperationInputValidator
import com.lykke.matching.engine.services.validators.input.LimitOrderCancelOperationInputValidator
import com.lykke.matching.engine.services.validators.input.impl.CashInOutOperationInputValidatorImpl
import com.lykke.matching.engine.services.validators.input.impl.CashTransferOperationInputValidatorImpl
import com.lykke.matching.engine.services.validators.input.impl.LimitOrderInputValidatorImpl
import com.lykke.matching.engine.services.validators.input.impl.LimitOrderCancelOperationInputValidatorImpl
import com.lykke.matching.engine.services.validators.settings.SettingValidator
import com.lykke.matching.engine.services.validators.settings.impl.DisabledFunctionalitySettingValidator
import com.lykke.matching.engine.services.validators.settings.impl.MessageProcessingSwitchSettingValidator
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import com.lykke.matching.engine.utils.order.AllOrdersCanceller
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import com.lykke.utils.logging.ThrottlingLogger
import com.nhaarman.mockito_kotlin.any
import org.mockito.Mockito
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.Optional
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.LinkedBlockingQueue

@Configuration
@Import(QueueConfig::class, TestExecutionContext::class, JsonConfig::class)
open class TestApplicationContext {

    @Bean
    open fun tradeInfoQueue(): BlockingQueue<TradeInfo> {
        return LinkedBlockingQueue<TradeInfo>()
    }

    @Bean
    open fun threadPoolTaskExecutor(): Executor {
        val threadPoolTaskExecutor = ThreadPoolTaskExecutor()
        threadPoolTaskExecutor.threadNamePrefix = "executor-task"
        threadPoolTaskExecutor.corePoolSize = 2
        threadPoolTaskExecutor.maxPoolSize = 2

        return threadPoolTaskExecutor
    }

    @Bean
    open fun balanceHolder(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                           persistenceManager: PersistenceManager,
                           balanceUpdateQueue: BlockingQueue<BalanceUpdate>,
                           applicationSettingsHolder: ApplicationSettingsHolder,
                           backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor): BalancesHolder {
        return BalancesHolder(balancesDatabaseAccessorsHolder, persistenceManager, assetHolder(backOfficeDatabaseAccessor),
                balanceUpdateQueue, applicationSettingsHolder)
    }

    @Bean
    open fun assetHolder(backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor): AssetsHolder {
        return AssetsHolder(assetCache(backOfficeDatabaseAccessor))
    }

    @Bean
    open fun applicationSettingsHolder(applicationSettingsCache: ApplicationSettingsCache): ApplicationSettingsHolder {
        return ApplicationSettingsHolder(applicationSettingsCache)
    }

    @Bean
    open fun messageSequenceNumberHolder(messageSequenceNumberDatabaseAccessor: ReadOnlyMessageSequenceNumberDatabaseAccessor): MessageSequenceNumberHolder {
        return MessageSequenceNumberHolder(messageSequenceNumberDatabaseAccessor)
    }

    @Bean
    open fun notificationSender(clientsEventsQueue: BlockingQueue<Event<*>>,
                                trustedClientsEventsQueue: BlockingQueue<ExecutionEvent>): MessageSender {
        return MessageSender(clientsEventsQueue, trustedClientsEventsQueue)
    }

    @Bean
    open fun reservedVolumesRecalculator(testOrderDatabaseAccessorHolder: OrdersDatabaseAccessorsHolder,
                                         stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                         testReservedVolumesDatabaseAccessor: TestReservedVolumesDatabaseAccessor,
                                         assetHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder,
                                         balancesHolder: BalancesHolder, applicationSettingsHolder: ApplicationSettingsHolder,
                                         messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                         messageSender: MessageSender): ReservedVolumesRecalculator {

        return ReservedVolumesRecalculator(testOrderDatabaseAccessorHolder, stopOrdersDatabaseAccessorsHolder,
                testReservedVolumesDatabaseAccessor, assetHolder,
                assetsPairsHolder, balancesHolder, applicationSettingsHolder,
                false, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun testMessageSequenceNumberDatabaseAccessor(): TestMessageSequenceNumberDatabaseAccessor {
        return TestMessageSequenceNumberDatabaseAccessor()
    }

    @Bean
    open fun testReservedVolumesDatabaseAccessor(): TestReservedVolumesDatabaseAccessor {
        return TestReservedVolumesDatabaseAccessor()
    }

    @Bean
    open fun assetCache(backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor): AssetsCache {
        return AssetsCache(backOfficeDatabaseAccessor)
    }

    @Bean
    open fun testBackOfficeDatabaseAccessor(): TestBackOfficeDatabaseAccessor {
        return TestBackOfficeDatabaseAccessor()
    }

    @Bean
    open fun testSettingsDatabaseAccessor(): SettingsDatabaseAccessor {
        return TestSettingsDatabaseAccessor()
    }

    @Bean
    open fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
        return TestDictionariesDatabaseAccessor()
    }

    @Bean
    open fun applicationSettingsCache(configDatabaseAccessor: SettingsDatabaseAccessor,
                                      applicationEventPublisher: ApplicationEventPublisher): ApplicationSettingsCache {
        return ApplicationSettingsCache(configDatabaseAccessor, applicationEventPublisher)
    }

    @Bean
    open fun balanceUpdateHandler(balanceUpdateQueue: BlockingQueue<BalanceUpdate>): BalanceUpdateHandlerTest {
        return BalanceUpdateHandlerTest(balanceUpdateQueue)
    }

    @Bean
    open fun testReservedCashOperationListener(): TestReservedCashOperationListener {
        return TestReservedCashOperationListener()
    }

    @Bean
    open fun testBalanceHolderWrapper(balanceUpdateHandlerTest: BalanceUpdateHandlerTest,
                                      balancesHolder: BalancesHolder): TestBalanceHolderWrapper {
        return TestBalanceHolderWrapper(balanceUpdateHandlerTest, balancesHolder)
    }

    @Bean
    open fun balancesDatabaseAccessorsHolder(): BalancesDatabaseAccessorsHolder {
        return BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor(), null)
    }

    @Bean
    open fun ordersDatabaseAccessorsHolder(testOrderBookDatabaseAccessor: TestOrderBookDatabaseAccessor,
                                           testFileOrderDatabaseAccessor: TestFileOrderDatabaseAccessor): OrdersDatabaseAccessorsHolder {
        return OrdersDatabaseAccessorsHolder(testOrderBookDatabaseAccessor, testFileOrderDatabaseAccessor)
    }

    @Bean
    open fun testOrderBookDatabaseAccessor(testFileOrderDatabaseAccessor: TestFileOrderDatabaseAccessor): TestOrderBookDatabaseAccessor {
        return TestOrderBookDatabaseAccessor(testFileOrderDatabaseAccessor)
    }

    @Bean
    open fun stopOrdersDatabaseAccessorsHolder(testStopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor,
                                               testFileStopOrderDatabaseAccessor: TestFileStopOrderDatabaseAccessor): StopOrdersDatabaseAccessorsHolder {
        return StopOrdersDatabaseAccessorsHolder(testStopOrderBookDatabaseAccessor, testFileStopOrderDatabaseAccessor)
    }

    @Bean
    open fun persistenceManager(ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder): PersistenceManager {
        return TestPersistenceManager(balancesDatabaseAccessorsHolder().primaryAccessor,
                ordersDatabaseAccessorsHolder,
                stopOrdersDatabaseAccessorsHolder)
    }

    @Bean
    open fun cashInOutOperationBusinessValidator(balancesHolder: BalancesHolder): CashInOutOperationBusinessValidator {
        return CashInOutOperationBusinessValidatorImpl(balancesHolder)
    }

    @Bean
    open fun cashTransferOperationBusinessValidator(balancesHolder: BalancesHolder): CashTransferOperationBusinessValidator {
        return CashTransferOperationBusinessValidatorImpl(balancesHolder)
    }

    @Bean
    open fun cashInOutOperationInputValidator(applicationSettingsHolder: ApplicationSettingsHolder): CashInOutOperationInputValidator {
        return CashInOutOperationInputValidatorImpl(applicationSettingsHolder)
    }

    @Bean
    open fun cashTransferOperationInputValidator(applicationSettingsHolder: ApplicationSettingsHolder): CashTransferOperationInputValidator {
        return CashTransferOperationInputValidatorImpl(applicationSettingsHolder)
    }

    @Bean
    open fun disabledFunctionality(assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder): DisabledFunctionalitySettingValidator {
        return DisabledFunctionalitySettingValidator(assetsHolder, assetsPairsHolder)
    }

    @Bean
    open fun cashInOutOperationService(balancesHolder: BalancesHolder,
                                       rabbitCashInOutQueue: BlockingQueue<CashOperation>,
                                       feeProcessor: FeeProcessor,
                                       cashInOutOperationBusinessValidator: CashInOutOperationBusinessValidator,
                                       messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                       messageSender: MessageSender): CashInOutOperationService {
        return CashInOutOperationService(balancesHolder, rabbitCashInOutQueue, feeProcessor,
                cashInOutOperationBusinessValidator, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun marketOrderValidator(assetsPairsHolder: AssetsPairsHolder,
                                  assetsHolder: AssetsHolder,
                                  applicationSettingsHolder: ApplicationSettingsHolder): MarketOrderValidator {
        return MarketOrderValidatorImpl(assetsPairsHolder, assetsHolder, applicationSettingsHolder)
    }

    @Bean
    open fun assetPairsCache(testDictionariesDatabaseAccessor: DictionariesDatabaseAccessor): AssetPairsCache {
        return AssetPairsCache(testDictionariesDatabaseAccessor)
    }

    @Bean
    open fun assetPairHolder(assetPairsCache: AssetPairsCache): AssetsPairsHolder {
        return AssetsPairsHolder(assetPairsCache)
    }

    @Bean
    open fun reservedCashInOutOperationValidator(balancesHolder: BalancesHolder,
                                                 assetsHolder: AssetsHolder): ReservedCashInOutOperationValidator {
        return ReservedCashInOutOperationValidatorImpl(assetsHolder, balancesHolder)
    }

    @Bean
    open fun reservedCashInOutOperation(balancesHolder: BalancesHolder,
                                        assetsHolder: AssetsHolder,
                                        reservedCashOperationQueue: BlockingQueue<ReservedCashOperation>,
                                        reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator,
                                        messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                        uuidHolder: UUIDHolder): ReservedCashInOutOperationService {
        return ReservedCashInOutOperationService(assetsHolder,
                balancesHolder,
                reservedCashOperationQueue,
                reservedCashInOutOperationValidator,
                messageProcessingStatusHolder,
                uuidHolder)
    }

    @Bean
    open fun applicationSettingsHistoryDatabaseAccessor(): SettingsHistoryDatabaseAccessor {
        return Mockito.mock(SettingsHistoryDatabaseAccessor::class.java)
    }

    @Bean
    open fun applicationSettingsService(settingsDatabaseAccessor: SettingsDatabaseAccessor,
                                        applicationSettingsCache: ApplicationSettingsCache,
                                        settingsHistoryDatabaseAccessor: SettingsHistoryDatabaseAccessor,
                                        applicationEventPublisher: ApplicationEventPublisher): ApplicationSettingsService {
        return ApplicationSettingsServiceImpl(settingsDatabaseAccessor, applicationSettingsCache, settingsHistoryDatabaseAccessor, applicationEventPublisher)
    }

    @Bean
    open fun disabledFunctionalityRulesHolder(applicationSettingsCache: ApplicationSettingsCache,
                                              assetsPairsHolder: AssetsPairsHolder): DisabledFunctionalityRulesHolder {
        return DisabledFunctionalityRulesHolder(applicationSettingsCache, assetsPairsHolder)
    }

    @Bean
    open fun genericLimitOrderService(testOrderDatabaseAccessor: OrdersDatabaseAccessorsHolder,
                                      tradeInfoQueue: Optional<BlockingQueue<TradeInfo>>,
                                      expiryOrdersQueue: ExpiryOrdersQueue): GenericLimitOrderService {
        return GenericLimitOrderService(testOrderDatabaseAccessor,
                tradeInfoQueue,
                expiryOrdersQueue)
    }

    @Bean
    open fun singleLimitOrderService(executionContextFactory: ExecutionContextFactory,
                                     genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
                                     stopOrderBookProcessor: StopOrderBookProcessor,
                                     executionDataApplyService: ExecutionDataApplyService,
                                     clientAccountsService: ClientAccountsService,
                                     previousLimitOrdersProcessor: PreviousLimitOrdersProcessor): SingleLimitOrderService {
        return SingleLimitOrderService(executionContextFactory,
                genericLimitOrdersProcessor,
                stopOrderBookProcessor,
                executionDataApplyService,
                clientAccountsService,
                previousLimitOrdersProcessor)
    }

    @Bean
    open fun clientAccountsServiceve(clientAccountsCache: ClientAccountsCache): ClientAccountsService {
        return ClientAccountsServiceImpl(clientAccountsCache)
    }

    @Bean
    open fun multiLimitOrderService(genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
                                    executionContextFactory: ExecutionContextFactory,
                                    previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
                                    stopOrderBookProcessor: StopOrderBookProcessor,
                                    executionDataApplyService: ExecutionDataApplyService,
                                    assetsHolder: AssetsHolder,
                                    assetsPairsHolder: AssetsPairsHolder,
                                    balancesHolder: BalancesHolder,
                                    applicationSettingsHolder: ApplicationSettingsHolder,
                                    clientAccountsService: ClientAccountsService,
                                    messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                    testUUIDHolder: TestUUIDHolder): MultiLimitOrderService {
        return MultiLimitOrderService(executionContextFactory,
                genericLimitOrdersProcessor,
                stopOrderBookProcessor,
                executionDataApplyService,
                previousLimitOrdersProcessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                clientAccountsService,
                applicationSettingsHolder,
                messageProcessingStatusHolder,
                testUUIDHolder)
    }

    @Bean
    open fun testUUIDHolder() = TestUUIDHolder()

    @Bean
    open fun marketOrderService(matchingEngine: MatchingEngine,
                                executionContextFactory: ExecutionContextFactory,
                                stopOrderBookProcessor: StopOrderBookProcessor,
                                executionDataApplyService: ExecutionDataApplyService,
                                matchingResultHandlingHelper: MatchingResultHandlingHelper,
                                genericLimitOrderService: GenericLimitOrderService,
                                assetsPairsHolder: AssetsPairsHolder,
                                rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
                                marketOrderValidator: MarketOrderValidator,
                                messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                messageSender: MessageSender,
                                applicationSettingsHolder: ApplicationSettingsHolder,
                                messageProcessingStatusHolder: MessageProcessingStatusHolder,
                                clientAccountsService: ClientAccountsService,
                                uuidHolder: UUIDHolder): MarketOrderService {
        return MarketOrderService(matchingEngine,
                executionContextFactory,
                stopOrderBookProcessor,
                executionDataApplyService,
                matchingResultHandlingHelper,
                genericLimitOrderService,
                assetsPairsHolder,
                rabbitSwapQueue,
                marketOrderValidator,
                applicationSettingsHolder,
                messageSequenceNumberHolder,
                messageSender,
                messageProcessingStatusHolder,
                clientAccountsService,
                uuidHolder)
    }

    @Bean
    open fun minVolumeOrderCanceller(assetsPairsHolder: AssetsPairsHolder,
                                     genericLimitOrderService: GenericLimitOrderService,
                                     limitOrdersCancelExecutor: LimitOrdersCancelExecutor): MinVolumeOrderCanceller {
        return MinVolumeOrderCanceller(assetsPairsHolder,
                genericLimitOrderService,
                limitOrdersCancelExecutor,
                true)
    }

    @Bean
    open fun genericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                          expiryOrdersQueue: ExpiryOrdersQueue): GenericStopLimitOrderService {
        return GenericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder, expiryOrdersQueue)
    }

    @Bean
    open fun testTrustedClientsLimitOrderListener(): TestTrustedClientsLimitOrderListener {
        return TestTrustedClientsLimitOrderListener()
    }

    @Bean
    open fun testStopOrderBookDatabaseAccessor(testFileStopOrderDatabaseAccessor: TestFileStopOrderDatabaseAccessor): TestStopOrderBookDatabaseAccessor {
        return TestStopOrderBookDatabaseAccessor(testFileStopOrderDatabaseAccessor)
    }

    @Bean
    open fun testFileOrderDatabaseAccessor(): TestFileOrderDatabaseAccessor {
        return TestFileOrderDatabaseAccessor()
    }

    @Bean
    open fun testFileStopOrderDatabaseAccessor(): TestFileStopOrderDatabaseAccessor {
        return TestFileStopOrderDatabaseAccessor()
    }

    @Bean
    open fun testClientLimitOrderListener(): TestClientLimitOrderListener {
        return TestClientLimitOrderListener()
    }

    @Bean
    open fun orderBookListener(): TestOrderBookListener {
        return TestOrderBookListener()
    }

    @Bean
    open fun rabbitOrderBookListener(): TestRabbitOrderBookListener {
        return TestRabbitOrderBookListener()
    }

    @Bean
    open fun lkkTradeListener(): TestLkkTradeListener {
        return TestLkkTradeListener()
    }

    @Bean
    open fun testOrderBookWrapper(genericLimitOrderService: GenericLimitOrderService,
                                  testOrderBookDatabaseAccessor: TestOrderBookDatabaseAccessor,
                                  genericStopLimitOrderService: GenericStopLimitOrderService,
                                  stopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor): TestOrderBookWrapper {
        return TestOrderBookWrapper(genericLimitOrderService, testOrderBookDatabaseAccessor, genericStopLimitOrderService, stopOrderBookDatabaseAccessor)
    }

    @Bean
    open fun rabbitSwapListener(): RabbitSwapListener {
        return RabbitSwapListener()
    }

    @Bean
    open fun tradeInfoListener(): TradeInfoListener {
        return TradeInfoListener()
    }

    @Bean
    open fun allOrdersCanceller(genericLimitOrderService: GenericLimitOrderService,
                                genericStopLimitOrderService: GenericStopLimitOrderService,
                                limitOrdersCancelExecutor: LimitOrdersCancelExecutor): AllOrdersCanceller {
        return AllOrdersCanceller(genericLimitOrderService,
                genericStopLimitOrderService,
                limitOrdersCancelExecutor,
                true)
    }

    @Bean
    open fun feeProcessor(assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder, genericLimitOrderService: GenericLimitOrderService): FeeProcessor {
        return FeeProcessor(assetsHolder, assetsPairsHolder, genericLimitOrderService)
    }

    @Bean
    open fun cashInOutContextParser(assetsHolder: AssetsHolder, uuidHolder: UUIDHolder): CashInOutContextParser {
        return CashInOutContextParser(assetsHolder, uuidHolder)
    }

    @Bean
    open fun processedMessagesCache(): ProcessedMessagesCache {
        return Mockito.mock(ProcessedMessagesCache::class.java)
    }

    @Bean
    open fun cashInOutPreprocessor(cashInOutContextParser: CashInOutContextParser,
                                   persistenceManager: PersistenceManager,
                                   processedMessagesCache: ProcessedMessagesCache,
                                   messageProcessingStatusHolder: MessageProcessingStatusHolder): CashInOutPreprocessor {
        return CashInOutPreprocessor(cashInOutContextParser,
                LinkedBlockingQueue(),
                Mockito.mock(CashOperationIdDatabaseAccessor::class.java),
                persistenceManager,
                processedMessagesCache,
                messageProcessingStatusHolder,
                ThrottlingLogger.getLogger("cashInOut"))
    }

    @Bean
    open fun cashTransferInitializer(assetsHolder: AssetsHolder, uuidHolder: UUIDHolder): CashTransferContextParser {
        return CashTransferContextParser(assetsHolder, uuidHolder)
    }

    @Bean
    open fun healthMonitor(): HealthMonitor {
        return Mockito.mock(HealthMonitor::class.java) {
            true
        }
    }

    @Bean
    open fun messageProcessingStatusHolder(generalHealthMonitor: HealthMonitor,
                                           applicationSettingsHolder: ApplicationSettingsHolder,
                                           disabledFunctionalityRulesHolder: DisabledFunctionalityRulesHolder): MessageProcessingStatusHolder {
        return MessageProcessingStatusHolder(generalHealthMonitor, applicationSettingsHolder, disabledFunctionalityRulesHolder)
    }

    @Bean
    open fun cashTransferPreprocessor(cashTransferContextParser: CashTransferContextParser,
                                      persistenceManager: PersistenceManager,
                                      processedMessagesCache: ProcessedMessagesCache,
                                      messageProcessingStatusHolder: MessageProcessingStatusHolder): CashTransferPreprocessor {
        return CashTransferPreprocessor(cashTransferContextParser,
                LinkedBlockingQueue(),
                Mockito.mock(CashOperationIdDatabaseAccessor::class.java),
                persistenceManager,
                processedMessagesCache,
                messageProcessingStatusHolder,
                ThrottlingLogger.getLogger("transfer"))
    }

    @Bean
    open fun messageBuilder(cashTransferContextParser: CashTransferContextParser,
                            cashInOutContextParser: CashInOutContextParser,
                            singleLimitOrderContextParser: SingleLimitOrderContextParser,
                            limitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData>,
                            limitOrderMassCancelOperationContextParser: ContextParser<LimitOrderMassCancelOperationParsedData>): MessageBuilder {
        return MessageBuilder(singleLimitOrderContextParser, cashInOutContextParser, cashTransferContextParser,
                limitOrderCancelOperationContextParser, limitOrderMassCancelOperationContextParser)
    }

    @Bean
    open fun cashTransferOperationService(balancesHolder: BalancesHolder, notification: BlockingQueue<CashTransferOperation>,
                                          dbTransferOperationQueue: BlockingQueue<TransferOperation>, feeProcessor: FeeProcessor,
                                          cashTransferOperationBusinessValidator: CashTransferOperationBusinessValidator, messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                          messageSender: MessageSender): CashTransferOperationService {
        return CashTransferOperationService(balancesHolder, notification, dbTransferOperationQueue, feeProcessor,
                cashTransferOperationBusinessValidator, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun settingsListener(): SettingsListener {
        return SettingsListener()
    }

    @Bean
    open fun messageProcessingSwitchSettingValidator(): SettingValidator {
        return MessageProcessingSwitchSettingValidator()
    }

    @Bean
    open fun settingValidators(settingValidators: List<SettingValidator>): Map<AvailableSettingGroup, List<SettingValidator>> {
        return settingValidators.groupBy { it.getSettingGroup() }
    }

    @Bean
    open fun singleLimitOrderContextParser(assetsPairsHolder: AssetsPairsHolder,
                                           assetsHolder: AssetsHolder,
                                           applicationSettingsHolder: ApplicationSettingsHolder,
                                           clientAccountsService: ClientAccountsService,
                                           uuidHolder: UUIDHolder): SingleLimitOrderContextParser {
        return SingleLimitOrderContextParser(assetsPairsHolder,
                assetsHolder,
                applicationSettingsHolder,
                uuidHolder,
                clientAccountsService,
                ThrottlingLogger.getLogger("limitOrder"))
    }

    @Bean
    open fun clientAccountsCache(): ClientAccountsCache {
        val clientAccountsCacheMock = Mockito.mock(ClientAccountsCache::class.java)
        Mockito.`when`(clientAccountsCacheMock.getClientByWalletId(any())).thenAnswer { invocation -> invocation.arguments[0] }
        Mockito.`when`(clientAccountsCacheMock.getWalletsByClientId(any())).thenAnswer { invocation -> setOf(invocation.arguments[0]) }
        return clientAccountsCacheMock
    }

    @Bean
    open fun limitOrderInputValidator(applicationSettingsHolder: ApplicationSettingsHolder): LimitOrderInputValidator {
        return LimitOrderInputValidatorImpl(applicationSettingsHolder)
    }


    @Bean
    open fun limitOrderBusinessValidator(): LimitOrderBusinessValidator {
        return LimitOrderBusinessValidatorImpl()
    }

    @Bean
    open fun stopOrderBusinessValidatorImpl(): StopOrderBusinessValidatorImpl {
        return StopOrderBusinessValidatorImpl()
    }

    @Bean
    open fun limitOrderCancelOperationInputValidator(): LimitOrderCancelOperationInputValidator {
        return LimitOrderCancelOperationInputValidatorImpl()
    }

    @Bean
    open fun limitOrderCancelOperationBusinessValidator(): LimitOrderCancelOperationBusinessValidator {
        return LimitOrderCancelOperationBusinessValidatorImpl()
    }

    @Bean
    open fun limitOrdersCancelServiceHelper(limitOrdersCancelExecutor: LimitOrdersCancelExecutor): LimitOrdersCancelServiceHelper {
        return LimitOrdersCancelServiceHelper(limitOrdersCancelExecutor)
    }

    @Bean
    open fun limitOrderCancelService(genericLimitOrderService: GenericLimitOrderService,
                                     genericStopLimitOrderService: GenericStopLimitOrderService,
                                     validator: LimitOrderCancelOperationBusinessValidator,
                                     limitOrdersCancelServiceHelper: LimitOrdersCancelServiceHelper): LimitOrderCancelService {
        return LimitOrderCancelService(genericLimitOrderService, genericStopLimitOrderService, validator, limitOrdersCancelServiceHelper)
    }

    @Bean
    open fun limitOrderCancelOperationContextParser(): LimitOrderCancelOperationContextParser {
        return LimitOrderCancelOperationContextParser()
    }

    @Bean
    open fun limitOrderMassCancelOperationContextParser(): ContextParser<LimitOrderMassCancelOperationParsedData> {
        return LimitOrderMassCancelOperationContextParser()
    }

    @Bean
    open fun limitOrderMassCancelService(genericLimitOrderService: GenericLimitOrderService,
                                         genericStopLimitOrderService: GenericStopLimitOrderService,
                                         limitOrdersCancelServiceHelper: LimitOrdersCancelServiceHelper): LimitOrderMassCancelService {
        return LimitOrderMassCancelService(genericLimitOrderService, genericStopLimitOrderService, limitOrdersCancelServiceHelper)
    }

    @Bean
    open fun multiLimitOrderCancelService(genericLimitOrderService: GenericLimitOrderService,
                                          limitOrdersCancelServiceHelper: LimitOrdersCancelServiceHelper,
                                          applicationSettingsHolder: ApplicationSettingsHolder): MultiLimitOrderCancelService {
        return MultiLimitOrderCancelService(genericLimitOrderService,
                limitOrdersCancelServiceHelper,
                applicationSettingsHolder)
    }

    @Bean
    open fun disabledFunctionalityRulesService(): DisabledFunctionalityRulesService {
        return DisabledFunctionalityRulesServiceImpl()
    }

    @Bean
    open fun singleLimitOrderPreprocessor(singleLimitOrderContextParser: SingleLimitOrderContextParser,
                                          preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
                                          messageProcessingStatusHolder: MessageProcessingStatusHolder): SingleLimitOrderPreprocessor {
        return SingleLimitOrderPreprocessor(singleLimitOrderContextParser,
                preProcessedMessageQueue,
                messageProcessingStatusHolder,
                ThrottlingLogger.getLogger("limitOrder"))
    }

    @Bean
    open fun expiryOrdersQueue() = ExpiryOrdersQueue()

    @Bean
    open fun messageRouter(limitOrderInputQueue: BlockingQueue<MessageWrapper>,
                           cashInOutInputQueue: BlockingQueue<MessageWrapper>,
                           cashTransferInputQueue: BlockingQueue<MessageWrapper>,
                           limitOrderCancelInputQueue: BlockingQueue<MessageWrapper>,
                           limitOrderMassCancelInputQueue: BlockingQueue<MessageWrapper>,
                           preProcessedMessageQueue: BlockingQueue<MessageWrapper>): MessageRouter {
        return MessageRouter(limitOrderInputQueue,
                cashInOutInputQueue,
                cashTransferInputQueue,
                limitOrderCancelInputQueue,
                limitOrderMassCancelInputQueue,
                preProcessedMessageQueue)
    }
}