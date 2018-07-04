package com.lykke.matching.engine.config

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.spring.QueueConfig
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.notification.TestReservedCashOperationListener
import com.lykke.matching.engine.outgoing.messages.v2.AbstractEvent
import com.lykke.matching.engine.services.BalanceUpdateService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.ReservedCashInOutOperationService
import com.lykke.matching.engine.services.validators.*
import com.lykke.matching.engine.services.validators.impl.*
import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
import org.springframework.context.ApplicationEventPublisher
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
    open fun messageSequenceNumberHolder(messageSequenceNumberDatabaseAccessor: MessageSequenceNumberDatabaseAccessor): MessageSequenceNumberHolder {
        return MessageSequenceNumberHolder(messageSequenceNumberDatabaseAccessor)
    }

    @Bean
    open fun notificationSender(clientsEventsQueue: BlockingQueue<AbstractEvent<*>>,
                                trustedClientsEventsQueue: BlockingQueue<AbstractEvent<*>>): MessageSender {
        return MessageSender(clientsEventsQueue, trustedClientsEventsQueue)
    }

    @Bean
    open fun reservedVolumesRecalculator(testFileOrderDatabaseAccessor :TestFileOrderDatabaseAccessor,
                                         testStopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor,
                                         testReservedVolumesDatabaseAccessor: TestReservedVolumesDatabaseAccessor,
                                         assetHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder,
                                         balancesHolder: BalancesHolder, applicationSettingsCache: ApplicationSettingsCache,
                                         applicationEventPublisher: ApplicationEventPublisher,
                                         messageSequenceNumberHolder: MessageSequenceNumberHolder,
                                         messageSender: MessageSender): ReservedVolumesRecalculator {

        return ReservedVolumesRecalculator(testFileOrderDatabaseAccessor, testStopOrderBookDatabaseAccessor,
                testReservedVolumesDatabaseAccessor,  assetHolder,
                assetsPairsHolder, balancesHolder, applicationSettingsCache,
                "tset", false, applicationEventPublisher, messageSequenceNumberHolder, messageSender)
    }

    @Bean
    open fun testMessageSequenceNumberDatabaseAccessor(): TestMessageSequenceNumberDatabaseAccessor {
        return TestMessageSequenceNumberDatabaseAccessor()
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
}