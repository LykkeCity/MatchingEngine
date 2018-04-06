package com.lykke.matching.engine.config

import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class TestApplicationContext {

    @Bean
    open fun balanceHolder(walletDatabaseAccessor: WalletDatabaseAccessor,
                      applicationEventPublisher: ApplicationEventPublisher, applicationSettingsCache: ApplicationSettingsCache,
                      backOfficeDatabaseAccessor: BackOfficeDatabaseAccessor): BalancesHolder {
        return BalancesHolder(walletDatabaseAccessor, assetHolder(backOfficeDatabaseAccessor),
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
    open fun config(): FactoryBean<Config> {
        return LocalConfig()
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
    open fun testWalletDatabaseAccessor(): TestWalletDatabaseAccessor {
        return TestWalletDatabaseAccessor()
    }

    @Bean
    open fun balanceUpdateHandler(): BalanceUpdateHandlerTest {
        return BalanceUpdateHandlerTest()
    }

    @Bean
    open fun applicationSettingsCache(configDatabaseAccessor: ConfigDatabaseAccessor): ApplicationSettingsCache {
        return ApplicationSettingsCache(configDatabaseAccessor, 60000)
    }
}