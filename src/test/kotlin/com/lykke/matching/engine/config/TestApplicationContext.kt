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
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.notification.TestReservedCashOperationListener
import com.lykke.matching.engine.services.ReservedCashInOutOperationService
import com.lykke.matching.engine.services.validators.*
import com.lykke.matching.engine.services.validators.impl.*
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
        return BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor(), null, null)
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
                                        reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator) :ReservedCashInOutOperationService {
        return ReservedCashInOutOperationService(assetsHolder, balancesHolder, applicationEventPublisher, reservedCashInOutOperationValidator)
    }

}