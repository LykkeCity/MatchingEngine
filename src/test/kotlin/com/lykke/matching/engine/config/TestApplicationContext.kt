package com.lykke.matching.engine.config

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.spring.LoggerConfig
import com.lykke.matching.engine.config.spring.QueueConfig
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
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
import com.lykke.matching.engine.notification.RabbitSwapListener
import com.lykke.matching.engine.notification.TestClientLimitOrderListener
import com.lykke.matching.engine.notification.TestLkkTradeListener
import com.lykke.matching.engine.notification.TestOrderBookListener
import com.lykke.matching.engine.notification.TestRabbitOrderBookListener
import com.lykke.matching.engine.notification.TestReservedCashOperationListener
import com.lykke.matching.engine.notification.TestTrustedClientsLimitOrderListener
import com.lykke.matching.engine.notification.TradeInfoListener
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.incoming.preprocessor.impl.CashInOutPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.CashTransferPreprocessor
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.*
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.*
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import com.lykke.matching.engine.services.validators.business.impl.LimitOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.business.CashInOutOperationBusinessValidator
import com.lykke.matching.engine.services.validators.business.CashTransferOperationBusinessValidator
import com.lykke.matching.engine.services.validators.business.impl.CashInOutOperationBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.business.impl.CashTransferOperationBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.impl.*
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import com.lykke.matching.engine.services.validators.input.impl.LimitOrderInputValidatorImpl
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.services.validators.input.CashInOutOperationInputValidator
import com.lykke.matching.engine.services.validators.input.CashTransferOperationInputValidator
import com.lykke.matching.engine.services.validators.input.impl.CashInOutOperationInputValidatorImpl
import com.lykke.matching.engine.services.validators.input.impl.CashTransferOperationInputValidatorImpl
import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
import com.lykke.matching.engine.utils.order.AllOrdersCanceller
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.log4j.Logger
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration
@Import(QueueConfig::class)
open class TestApplicationContext {

    @Bean
    open fun balanceHolder(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                           persistenceManager: PersistenceManager,
                           balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>,
                           balanceUpdateQueue: BlockingQueue<BalanceUpdate>,
                           applicationSettingsCache: ApplicationSettingsCache,
                           backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor): BalancesHolder {
        return BalancesHolder(balancesDatabaseAccessorsHolder, persistenceManager, assetHolder(backOfficeDatabaseAccessor),
                balanceUpdateNotificationQueue, balanceUpdateQueue, applicationSettingsCache)
    }

    @Bean
    open fun assetHolder(backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor): AssetsHolder {
        return AssetsHolder(assetCache(backOfficeDatabaseAccessor))
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
                                         balancesHolder: BalancesHolder, applicationSettingsCache: ApplicationSettingsCache,
                                         balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>,
                                         messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                         messageSender: MessageSender): ReservedVolumesRecalculator {

        return ReservedVolumesRecalculator(testOrderDatabaseAccessorHolder, stopOrdersDatabaseAccessorsHolder,
                testReservedVolumesDatabaseAccessor,  assetHolder,
                assetsPairsHolder, balancesHolder, applicationSettingsCache,
                false, balanceUpdateNotificationQueue, messageSequenceNumberHolder, messageSender)
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
    open fun testConfigDatabaseAccessor(): ConfigDatabaseAccessor {
        return TestConfigDatabaseAccessor()
    }

    @Bean
    open fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
        return TestDictionariesDatabaseAccessor()
    }

    @Bean
    open fun applicationSettingsCache(configDatabaseAccessor: ConfigDatabaseAccessor): ApplicationSettingsCache {
        return ApplicationSettingsCache(configDatabaseAccessor, 60000)
    }

    @Bean
    open fun balanceUpdateHandler(balanceUpdateQueue: BlockingQueue<BalanceUpdate>,
                                  balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>): BalanceUpdateHandlerTest {
        return BalanceUpdateHandlerTest(balanceUpdateQueue, balanceUpdateNotificationQueue)
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
    open fun stopOrdersDatabaseAccessorsHolder(testStopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor): StopOrdersDatabaseAccessorsHolder {
        return StopOrdersDatabaseAccessorsHolder(testStopOrderBookDatabaseAccessor, null)
    }

    @Bean
    open fun persistenceManager(ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
                                stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder): PersistenceManager {
        return TestPersistenceManager(balancesDatabaseAccessorsHolder().primaryAccessor,
                ordersDatabaseAccessorsHolder,
                stopOrdersDatabaseAccessorsHolder.primaryAccessor)
    }

    @Bean
    open fun cashOperationValidator(balancesHolder: BalancesHolder,
                                    assetsHolder: AssetsHolder,
                                    applicationSettingsCache: ApplicationSettingsCache): CashOperationValidator {
        return CashOperationValidatorImpl(balancesHolder, assetsHolder, applicationSettingsCache)
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
    open fun cashInOutOperationInputValidator(balancesHolder: BalancesHolder, applicationSettingsCache: ApplicationSettingsCache): CashInOutOperationInputValidator {
        return CashInOutOperationInputValidatorImpl(balancesHolder, applicationSettingsCache)
    }

    @Bean
    open fun cashTransferOperationInputValidator(assetsHolder: AssetsHolder, applicationSettingsCache: ApplicationSettingsCache): CashTransferOperationInputValidator {
        return CashTransferOperationInputValidatorImpl(assetsHolder, applicationSettingsCache)
    }

    @Bean
    open fun cashSwapOperationValidator(balancesHolder: BalancesHolder,
                                        assetsHolder: AssetsHolder): CashSwapOperationValidator {
        return CashSwapOperationValidatorImpl(balancesHolder, assetsHolder)
    }

    @Bean
    open fun marketOrderValidator(limitOrderInputValidator: LimitOrderInputValidator,
                                  assetsPairsHolder: AssetsPairsHolder,
                                  assetsHolder: AssetsHolder,
                                  assetSettingsCache: ApplicationSettingsCache): MarketOrderValidator {
        return MarketOrderValidatorImpl(limitOrderInputValidator, assetsPairsHolder, assetsHolder, assetSettingsCache)
    }

    @Bean
    open fun assetPairsCache(testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor): AssetPairsCache {
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
                                        reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator): ReservedCashInOutOperationService {
        return ReservedCashInOutOperationService(assetsHolder, balancesHolder, reservedCashOperationQueue, reservedCashInOutOperationValidator)
    }

    @Bean
    open fun multiLimitOrderValidator(assetsHolder: AssetsHolder, limitOrderInputValidator: LimitOrderInputValidator,
                                      assetPairHolder: AssetsPairsHolder): MultiLimitOrderValidator {
        return MultiLimitOrderValidatorImpl(assetsHolder, assetPairHolder, limitOrderInputValidator)
    }

    @Bean
    open fun balanceUpdateValidator(balancesHolder: BalancesHolder, assetsHolder: AssetsHolder): BalanceUpdateValidator {
        return BalanceUpdateValidatorImpl(balancesHolder, assetsHolder)
    }

    @Bean
    open fun balance(balancesHolder: BalancesHolder, balanceUpdateValidator: BalanceUpdateValidator): BalanceUpdateService {
        return BalanceUpdateService(balancesHolder, balanceUpdateValidator)
    }

    @Bean
    open fun genericLimitOrderService(testOrderDatabaseAccessor: OrdersDatabaseAccessorsHolder,
                                      assetsHolder: AssetsHolder,
                                      assetsPairsHolder: AssetsPairsHolder,
                                      balancesHolder: BalancesHolder,
                                      quotesUpdateQueue: BlockingQueue<QuotesUpdate>,
                                      tradeInfoQueue: BlockingQueue<TradeInfo>,
                                      applicationSettingsCache: ApplicationSettingsCache): GenericLimitOrderService {
        return GenericLimitOrderService(testOrderDatabaseAccessor,
                assetsHolder,
                assetsPairsHolder,
                balancesHolder,
                quotesUpdateQueue,
                tradeInfoQueue,
                applicationSettingsCache)
    }

    @Bean
    open fun limitOrdersProcessorFactory(assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder,
                                         balancesHolder: BalancesHolder, genericLimitOrderService: GenericLimitOrderService,
                                         applicationSettingsCache: ApplicationSettingsCache,
                                         limitOrderInputValidator: LimitOrderInputValidator,
                                         settingsCache: ApplicationSettingsCache,
                                         clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                         lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                         orderBookQueue: BlockingQueue<OrderBook>,
                                         rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                         trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                         messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                         limitOrderBusinessValidator: LimitOrderBusinessValidator,
                                         messageSender: MessageSender): LimitOrdersProcessorFactory {
        return LimitOrdersProcessorFactory(balancesHolder, limitOrderBusinessValidator, limitOrderInputValidator, settingsCache, genericLimitOrderService, clientLimitOrdersQueue,
                lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue, trustedClientsLimitOrdersQueue, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun genericLimitOrderProcessorFactory(genericLimitOrderService: GenericLimitOrderService, genericStopLimitOrderService: GenericStopLimitOrderService,
                                               limitOrderProcessorFactory: LimitOrdersProcessorFactory, balancesHolder: BalancesHolder,
                                               clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>, assetsHolder: AssetsHolder,
                                               assetsPairsHolder: AssetsPairsHolder, feeProcessor: FeeProcessor, messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                               messageSender: MessageSender, limitOrderBusinessValidator: LimitOrderBusinessValidator, singleLimitOrderContextParser: SingleLimitOrderContextParser): GenericLimitOrderProcessorFactory {
        return GenericLimitOrderProcessorFactory(genericLimitOrderService, genericStopLimitOrderService, limitOrderProcessorFactory, limitOrderBusinessValidator, assetsHolder, assetsPairsHolder, balancesHolder,
                clientLimitOrdersQueue, feeProcessor, singleLimitOrderContextParser, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun multiLimitOrderService(genericLimitOrderService: GenericLimitOrderService, genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
                                    limitOrderProcessorFactory: LimitOrdersProcessorFactory,
                                    clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                    trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                    orderBookQueue: BlockingQueue<OrderBook>,
                                    rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                    lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                    assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder,
                                    genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory, multiLimitOrderValidator: MultiLimitOrderValidator,
                                    feeProcessor: FeeProcessor, messageSequenceNumberHolder: MessageSequenceNumberHolder, messageSender: MessageSender,
                                    applicationSettingsCache: ApplicationSettingsCache): MultiLimitOrderService {
        return MultiLimitOrderService(genericLimitOrderService, genericLimitOrdersCancellerFactory, limitOrderProcessorFactory,
                clientLimitOrdersQueue, trustedClientsLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue,
                assetsHolder, assetsPairsHolder, balancesHolder, genericLimitOrderProcessorFactory, multiLimitOrderValidator, feeProcessor,
                applicationSettingsCache,
                messageSequenceNumberHolder,
                messageSender)
    }

    @Bean
    open fun marketOrderService(genericLimitOrderService: GenericLimitOrderService, assetsHolder: AssetsHolder,
                                assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder, clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                orderBookQueue: BlockingQueue<OrderBook>,
                                rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
                                lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory, marketOrderValidator: MarketOrderValidator,
                                feeProcessor: FeeProcessor,
                                messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                messageSender: MessageSender,
                                applicationSettingsCache: ApplicationSettingsCache): MarketOrderService {
        return MarketOrderService(genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, clientLimitOrdersQueue, trustedClientsLimitOrdersQueue,
                lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue, genericLimitOrderProcessorFactory, marketOrderValidator, feeProcessor, applicationSettingsCache, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun genericLimitOrdersCancellerFactory(dictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor,
                                                assetsHolder: AssetsHolder,
                                                assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder,
                                                genericLimitOrderService: GenericLimitOrderService, genericStopLimitOrderService: GenericStopLimitOrderService,
                                                genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                                                orderBookQueue: BlockingQueue<OrderBook>,
                                                rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                                clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                messageSequenceNumberHolder: MessageSequenceNumberHolder, messageSender: MessageSender): GenericLimitOrdersCancellerFactory {
        return GenericLimitOrdersCancellerFactory(dictionariesDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, genericLimitOrderService,
                genericStopLimitOrderService, genericLimitOrderProcessorFactory, orderBookQueue, rabbitOrderBookQueue, clientLimitOrdersQueue, trustedClientsLimitOrdersQueue, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun minVolumeOrderCanceller(assetsPairsHolder: AssetsPairsHolder, genericLimitOrderService: GenericLimitOrderService,
                                     genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory): MinVolumeOrderCanceller {
        return MinVolumeOrderCanceller(assetsPairsHolder, genericLimitOrderService, genericLimitOrdersCancellerFactory, true)
    }

    @Bean
    open fun genericStopLimitOrderService(persistenceManager: TestPersistenceManager,
                                          stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
                                          genericLimitOrderService: GenericLimitOrderService): GenericStopLimitOrderService {
        return GenericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder, genericLimitOrderService, persistenceManager)
    }

    @Bean
    open fun testTrustedClientsLimitOrderListener(): TestTrustedClientsLimitOrderListener {
        return TestTrustedClientsLimitOrderListener()
    }

    @Bean
    open fun testStopOrderBookDatabaseAccessor(): TestStopOrderBookDatabaseAccessor {
        return TestStopOrderBookDatabaseAccessor()
    }

    @Bean
    open fun testFileOrderDatabaseAccessor(): TestFileOrderDatabaseAccessor {
        return TestFileOrderDatabaseAccessor()
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
    open fun allOrdersCanceller(assetsPairsHolder: AssetsPairsHolder, genericLimitOrderService: GenericLimitOrderService,
                                genericStopLimitOrderService: GenericStopLimitOrderService, genericLimitOrdersCancellerFactory:
                                GenericLimitOrdersCancellerFactory): AllOrdersCanceller {
        return AllOrdersCanceller(assetsPairsHolder, genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory, true)
    }

    @Bean
    open fun feeProcessor(balancesHolder: BalancesHolder, assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder, genericLimitOrderService: GenericLimitOrderService): FeeProcessor {
        return FeeProcessor(balancesHolder, assetsHolder, assetsPairsHolder, genericLimitOrderService)
    }


    @Bean
    open fun cashInOutContextParser(assetsHolder: AssetsHolder): CashInOutContextParser {
        return CashInOutContextParser(assetsHolder)
    }


    @Bean
    open fun cashInOutPreprocessor(applicationContext: ApplicationContext): CashInOutPreprocessor {
        return CashInOutPreprocessor(LinkedBlockingQueue(), LinkedBlockingQueue(),
                Mockito.mock(CashOperationIdDatabaseAccessor::class.java))
    }

    @Bean
    open fun cashTransferInitializer(assetsHolder: AssetsHolder): CashTransferContextParser {
        return CashTransferContextParser(assetsHolder)
    }

    @Bean
    open fun cashTransferPreprocessor(applicationContext: ApplicationContext): CashTransferPreprocessor {
        return CashTransferPreprocessor(LinkedBlockingQueue(), LinkedBlockingQueue(), Mockito.mock(CashOperationIdDatabaseAccessor::class.java))
    }

    @Bean
    open fun messageBuilder(cashTransferContextParser: CashTransferContextParser,
                            cashInOutContextParser: CashInOutContextParser, singleLimitOrderContextParser: SingleLimitOrderContextParser): MessageBuilder {
        return MessageBuilder(cashInOutContextParser, cashTransferContextParser, singleLimitOrderContextParser)
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
    open fun singleLimitOrderContextParser(assetsPairsHolder: AssetsPairsHolder, assetsHolder: AssetsHolder,
                                           applicationSettingsCache: ApplicationSettingsCache,
                                           @Qualifier("singleLimitOrderContextPreprocessorLogger")
                                           logger: ThrottlingLogger): SingleLimitOrderContextParser {
        return SingleLimitOrderContextParser(assetsPairsHolder, assetsHolder, applicationSettingsCache, logger)
    }

    @Bean
    open fun limitOrderInputValidator(): LimitOrderInputValidator {
        return LimitOrderInputValidatorImpl()
    }

    @Bean
    open fun limitOrderBusinessValidator(): LimitOrderBusinessValidator {
        return LimitOrderBusinessValidatorImpl()
    }

    @Bean(destroyMethod = "")
    open fun appStarterLogger(): Logger {
        return Logger.getLogger("AppStarter")
    }

    @Bean
    open fun singleLimitOrderContextPreprocessorLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger(SingleLimitOrderPreprocessor::class.java.name)
    }
}