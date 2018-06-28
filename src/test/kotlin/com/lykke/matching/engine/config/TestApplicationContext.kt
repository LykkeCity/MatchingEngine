package com.lykke.matching.engine.config

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.*
import com.lykke.matching.engine.order.GenericLimitOrderProcessorFactory
import com.lykke.matching.engine.order.cancel.GenericLimitOrdersCancellerFactory
import com.lykke.matching.engine.order.process.LimitOrdersProcessorFactory
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.notification.TestReservedCashOperationListener
import com.lykke.matching.engine.services.BalanceUpdateService
import com.lykke.matching.engine.services.ReservedCashInOutOperationService
import com.lykke.matching.engine.services.validators.*
import com.lykke.matching.engine.services.validators.impl.*
import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
import com.lykke.matching.engine.utils.config.RedisConfig
import com.lykke.matching.engine.utils.order.AllOrdersCanceller
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class TestApplicationContext {

    @Bean
    open fun balanceHolder(balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
                           persistenceManager: PersistenceManager,
                           applicationEventPublisher: ApplicationEventPublisher, applicationSettingsCache: ApplicationSettingsCache,
                           backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor): BalancesHolder {
        return BalancesHolder(balancesDatabaseAccessorsHolder, persistenceManager, assetHolder(backOfficeDatabaseAccessor),
                applicationEventPublisher, applicationSettingsCache)
    }

    @Bean
    open fun assetHolder(backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor): AssetsHolder {
        return AssetsHolder(assetCache(backOfficeDatabaseAccessor))
    }

    @Bean
    open fun reservedVolumesRecalculator(testOrderDatabaseAccessorHolder: OrdersDatabaseAccessorsHolder,
                                         testStopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor,
                                         testReservedVolumesDatabaseAccessor: TestReservedVolumesDatabaseAccessor,
                                         assetHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder,
                                         balancesHolder: BalancesHolder, applicationSettingsCache: ApplicationSettingsCache,
                                         applicationEventPublisher: ApplicationEventPublisher): ReservedVolumesRecalculator {

        return ReservedVolumesRecalculator(testOrderDatabaseAccessorHolder, testStopOrderBookDatabaseAccessor,
                testReservedVolumesDatabaseAccessor,  assetHolder,
                assetsPairsHolder, balancesHolder, applicationSettingsCache,
                "tset", false, applicationEventPublisher)
    }

    @Bean
    open fun testStopOrderBookDatabaseAccessor(): TestStopOrderBookDatabaseAccessor {
        return TestStopOrderBookDatabaseAccessor()
    }

    @Bean
    open fun testReservedVolumesDatabaseAccessor(): TestReservedVolumesDatabaseAccessor {
        return TestReservedVolumesDatabaseAccessor()
    }

    @Bean
    open fun testFileOrderDatabaseAccessor(): TestFileOrderDatabaseAccessor {
        return TestFileOrderDatabaseAccessor()
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
    open fun balanceUpdateHandler(): BalanceUpdateHandlerTest {
        return BalanceUpdateHandlerTest()
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
    open fun stopOrdersDatabaseAccessorsHolder(): StopOrdersDatabaseAccessorsHolder {
        return StopOrdersDatabaseAccessorsHolder(TestStopOrderBookDatabaseAccessor(), null)
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
                                        applicationEventPublisher: ApplicationEventPublisher,
                                        reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator): ReservedCashInOutOperationService {
        return ReservedCashInOutOperationService(assetsHolder, balancesHolder, applicationEventPublisher, reservedCashInOutOperationValidator)
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
                                      applicationEventPublisher: ApplicationEventPublisher, applicationSettingsCache: ApplicationSettingsCache): GenericLimitOrderService {
        return GenericLimitOrderService(testOrderDatabaseAccessor, assetsHolder, assetsPairsHolder, balancesHolder, applicationEventPublisher, applicationSettingsCache)
    }

    @Bean
    open fun limitOrdersProcessorFactory(assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder,
                                         balancesHolder: BalancesHolder, genericLimitOrderService: GenericLimitOrderService,
                                         applicationSettingsCache: ApplicationSettingsCache, applicationEventPublisher: ApplicationEventPublisher): LimitOrdersProcessorFactory {
        return LimitOrdersProcessorFactory(assetsHolder, assetsPairsHolder, balancesHolder, genericLimitOrderService, applicationSettingsCache, applicationEventPublisher)
    }

    @Bean
    open fun genericLimitOrderProcessorFactory(genericLimitOrderService: GenericLimitOrderService, genericStopLimitOrderService: GenericStopLimitOrderService,
                                               limitOrderProcessorFactory: LimitOrdersProcessorFactory, applicationEventPublisher: ApplicationEventPublisher,
                                               assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder, applicationSettingsCache: ApplicationSettingsCache): GenericLimitOrderProcessorFactory {
        return GenericLimitOrderProcessorFactory(genericLimitOrderService, genericStopLimitOrderService, limitOrderProcessorFactory, applicationEventPublisher, assetsHolder, assetsPairsHolder, balancesHolder, applicationSettingsCache)
    }

    @Bean
    open fun multiLimitOrderService(genericLimitOrderService: GenericLimitOrderService, genericLimitOrdersCancellerFactory: GenericLimitOrdersCancellerFactory,
                                    limitOrderProcessorFactory: LimitOrdersProcessorFactory, applicationEventPublisher: ApplicationEventPublisher,
                                    assetsHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder, genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory, multiLimitOrderValidator: MultiLimitOrderValidator): MultiLimitOrderService {
        return MultiLimitOrderService(genericLimitOrderService, genericLimitOrdersCancellerFactory, limitOrderProcessorFactory, applicationEventPublisher, assetsHolder, assetsPairsHolder, balancesHolder, genericLimitOrderProcessorFactory, multiLimitOrderValidator)
    }

    @Bean
    open fun marketOrderService(genericLimitOrderService: GenericLimitOrderService, assetsHolder: AssetsHolder,
                                assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder, applicationEventPublisher: ApplicationEventPublisher,
                                genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory, marketOrderValidator: MarketOrderValidator): MarketOrderService {
        return MarketOrderService(genericLimitOrderService, assetsHolder, assetsPairsHolder, balancesHolder, applicationEventPublisher, genericLimitOrderProcessorFactory, marketOrderValidator)
    }

    @Bean
    open fun genericLimitOrdersCancellerFactory(dictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor, assetsPairsHolder: AssetsPairsHolder, balancesHolder: BalancesHolder,
                                                genericLimitOrderService: GenericLimitOrderService, genericStopLimitOrderService: GenericStopLimitOrderService,
                                                genericLimitOrderProcessorFactory: GenericLimitOrderProcessorFactory, applicationEventPublisher: ApplicationEventPublisher): GenericLimitOrdersCancellerFactory {
        return GenericLimitOrdersCancellerFactory(dictionariesDatabaseAccessor, assetsPairsHolder, balancesHolder, genericLimitOrderService,
                genericStopLimitOrderService, genericLimitOrderProcessorFactory, applicationEventPublisher)
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
    open fun reservedVolumesRecalculator(orderBookDatabaseAccessor: OrderBookDatabaseAccessor, stopOrderBookDatabaseAccessor: StopOrderBookDatabaseAccessor,
                                         reservedVolumesDatabaseAccessor: ReservedVolumesDatabaseAccessor, applicationContext: ApplicationContext, applicationEventPublisher: ApplicationEventPublisher): ReservedVolumesRecalculator {
        return ReservedVolumesRecalculator(orderBookDatabaseAccessor,
                stopOrderBookDatabaseAccessor, reservedVolumesDatabaseAccessor, applicationContext, applicationEventPublisher)
    }

    @Bean
    open fun reservedVolumesDatabaseAccessor(): ReservedVolumesDatabaseAccessor {
        return TestReservedVolumesDatabaseAccessor()
    }

    @Bean
    open fun allOrdersCanceller(assetsPairsHolder: AssetsPairsHolder, genericLimitOrderService: GenericLimitOrderService,
                                genericStopLimitOrderService: GenericStopLimitOrderService, genericLimitOrdersCancellerFactory:
                                GenericLimitOrdersCancellerFactory): AllOrdersCanceller {
        return AllOrdersCanceller(assetsPairsHolder, genericLimitOrderService, genericStopLimitOrderService, genericLimitOrdersCancellerFactory, false)
    }
}