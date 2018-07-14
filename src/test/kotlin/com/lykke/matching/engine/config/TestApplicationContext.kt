package com.lykke.matching.engine.config

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.spring.QueueConfig
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.notification.*
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.*
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.*
import com.lykke.matching.engine.services.validators.impl.*
import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
import com.lykke.matching.engine.utils.order.AllOrdersCanceller
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import java.util.concurrent.BlockingQueue

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
    open fun reservedVolumesRecalculator(testFileOrderDatabaseAccessor :TestFileOrderDatabaseAccessor,
                                         testStopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor,
                                         testReservedVolumesDatabaseAccessor: TestReservedVolumesDatabaseAccessor,
                                         assetHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder,
                                         balancesHolder: BalancesHolder, applicationSettingsCache: ApplicationSettingsCache,
                                         balanceUpdateNotificationQueue: BlockingQueue<BalanceUpdateNotification>,
                                         messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                         messageSender: MessageSender): ReservedVolumesRecalculator {

        return ReservedVolumesRecalculator(testFileOrderDatabaseAccessor, testStopOrderBookDatabaseAccessor,
                testReservedVolumesDatabaseAccessor,  assetHolder,
                assetsPairsHolder, balancesHolder, applicationSettingsCache,
                "tset", false, balanceUpdateNotificationQueue, messageSequenceNumberHolder, messageSender)
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
    open fun persistenceManager(): PersistenceManager {
        return TestPersistenceManager(balancesDatabaseAccessorsHolder().primaryAccessor)
    }

    @Bean
    open fun cashOperationValidator(balancesHolder: BalancesHolder,
                                    assetsHolder: AssetsHolder,
                                    applicationSettingsCache: ApplicationSettingsCache): CashOperationValidator {
        return CashOperationValidatorImpl(balancesHolder, assetsHolder, applicationSettingsCache)
    }

    @Bean
    open fun cashInOutOperationValidator(balancesHolder: BalancesHolder,
                                         assetsHolder: AssetsHolder,
                                         applicationSettingsCache: ApplicationSettingsCache): CashInOutOperationValidator {
        return CashInOutOperationValidatorImpl(balancesHolder, assetsHolder, applicationSettingsCache)
    }

    @Bean
    open fun cashTransferOperationValidator(balancesHolder: BalancesHolder,
                                            assetsHolder: AssetsHolder,
                                            applicationSettingsCache: ApplicationSettingsCache): CashTransferOperationValidator {
        return CashTransferOperationValidatorImpl(balancesHolder, assetsHolder, applicationSettingsCache)
    }

    @Bean
    open fun cashSwapOperationValidator(balancesHolder: BalancesHolder,
                                        assetsHolder: AssetsHolder): CashSwapOperationValidator {
        return CashSwapOperationValidatorImpl(balancesHolder, assetsHolder)
    }

    @Bean
    open fun marketOrderValidator(assetsPairsHolder: AssetsPairsHolder,
                                  assetsHolder: AssetsHolder,
                                  assetSettingsCache: ApplicationSettingsCache): MarketOrderValidator {
        return MarketOrderValidatorImpl(assetsPairsHolder, assetsHolder, assetSettingsCache)
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
    open fun multiLimitOrderValidator(assetsHolder: AssetsHolder): MultiLimitOrderValidator {
        return MultiLimitOrderValidatorImpl(assetsHolder)
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
    open fun genericLimitOrderService(testOrderDatabaseAccessor: TestFileOrderDatabaseAccessor, assetsHolder: AssetsHolder,
                                      assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder,
                                      quotesUpdateQueue: BlockingQueue<QuotesUpdate>, tradeInfoQueue: BlockingQueue<TradeInfo>,
                                      applicationSettingsCache: ApplicationSettingsCache): GenericLimitOrderService {
        return GenericLimitOrderService(testOrderDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder,
                quotesUpdateQueue, tradeInfoQueue, applicationSettingsCache)
    }

    @Bean
    open fun limitOrdersProcessorFactory(assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder,
                                         balancesHolder: BalancesHolder, genericLimitOrderService: GenericLimitOrderService,
                                         applicationSettingsCache: ApplicationSettingsCache,
                                         clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                         lkkTradesQueue: BlockingQueue<List<LkkTrade>>,
                                         orderBookQueue: BlockingQueue<OrderBook>,
                                         rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                         trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                         messageSequenceNumberHolder: MessageSequenceNumberHolder, messageSender: MessageSender): LimitOrdersProcessorFactory {
        return LimitOrdersProcessorFactory(assetsHolder, assetsPairsHolder, balancesHolder, genericLimitOrderService, clientLimitOrdersQueue,
                lkkTradesQueue,
                orderBookQueue,
                rabbitOrderBookQueue,
                trustedClientsLimitOrdersQueue, applicationSettingsCache, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun genericLimitOrderProcessorFactory(genericLimitOrderService: GenericLimitOrderService, genericStopLimitOrderService: GenericStopLimitOrderService,
                                               limitOrderProcessorFactory: LimitOrdersProcessorFactory,
                                               assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder,
                                               applicationSettingsCache: ApplicationSettingsCache, clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                               messageSequenceNumberHolder: MessageSequenceNumberHolder, messageSender: MessageSender): GenericLimitOrderProcessorFactory {
        return GenericLimitOrderProcessorFactory(genericLimitOrderService, genericStopLimitOrderService, limitOrderProcessorFactory, assetsHolder, assetsPairsHolder, balancesHolder,
                applicationSettingsCache, clientLimitOrdersQueue, messageSequenceNumberHolder, messageSender)
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
                                    messageSequenceNumberHolder: MessageSequenceNumberHolder, messageSender: MessageSender): MultiLimitOrderService {
        return MultiLimitOrderService(genericLimitOrderService, genericLimitOrdersCancellerFactory, limitOrderProcessorFactory,
                clientLimitOrdersQueue, trustedClientsLimitOrdersQueue, lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue,
                assetsHolder, assetsPairsHolder, balancesHolder, genericLimitOrderProcessorFactory, multiLimitOrderValidator, messageSequenceNumberHolder, messageSender)
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
                                messageSequenceNumberHolder: MessageSequenceNumberHolder, messageSender: MessageSender): MarketOrderService {
        return MarketOrderService(genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, clientLimitOrdersQueue, trustedClientsLimitOrdersQueue,
                lkkTradesQueue, orderBookQueue, rabbitOrderBookQueue, rabbitSwapQueue, genericLimitOrderProcessorFactory, marketOrderValidator, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun genericLimitOrdersCancellerFactory(dictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor, assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder,
                                                genericLimitOrderService: GenericLimitOrderService, genericStopLimitOrderService: GenericStopLimitOrderService,
                                                genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory,
                                                orderBookQueue: BlockingQueue<OrderBook>,
                                                rabbitOrderBookQueue: BlockingQueue<OrderBook>,
                                                clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
                                                messageSequenceNumberHolder: MessageSequenceNumberHolder, messageSender: MessageSender): GenericLimitOrdersCancellerFactory {
        return GenericLimitOrdersCancellerFactory(dictionariesDatabaseAccessor, assetsPairsHolder, balancesHolder, genericLimitOrderService,
                genericStopLimitOrderService, genericLimitOrderProcessorFactory, orderBookQueue, rabbitOrderBookQueue, clientLimitOrdersQueue, trustedClientsLimitOrdersQueue, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun minVolumeOrderCanceller(assetsPairsHolder: AssetsPairsHolder, genericLimitOrderService: GenericLimitOrderService,
                                     genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory): MinVolumeOrderCanceller {
        return MinVolumeOrderCanceller(assetsPairsHolder, genericLimitOrderService, genericLimitOrdersCancellerFactory, false)
    }

    @Bean
    open fun genericStopLimitOrderService(stopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor, genericLimitOrderService: GenericLimitOrderService): GenericStopLimitOrderService {
        return GenericStopLimitOrderService(stopOrderBookDatabaseAccessor, genericLimitOrderService)
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
                                  testFileOrderDatabaseAccessor: TestFileOrderDatabaseAccessor,
                                  genericStopLimitOrderService: GenericStopLimitOrderService,
                                  stopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor): TestOrderBookWrapper {
        return TestOrderBookWrapper(genericLimitOrderService, testFileOrderDatabaseAccessor, genericStopLimitOrderService, stopOrderBookDatabaseAccessor)
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
        return AllOrdersCanceller(assetsPairsHolder, genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory, false)
    }
}