package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.WalletDatabaseAccessorFactory
import com.lykke.matching.engine.database.common.DefaultPersistenceManager
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class DatabaseAccessorConfig {

    @Autowired
    private lateinit var config: Config

    @Bean
    open fun balancesDatabaseAccessorsHolder(): BalancesDatabaseAccessorsHolder {
        return WalletDatabaseAccessorFactory(config.me).createAccessorsHolder()
    }

    @Bean
    open fun persistenceManager(): PersistenceManager {
        return DefaultPersistenceManager(balancesDatabaseAccessorsHolder())
    }
}