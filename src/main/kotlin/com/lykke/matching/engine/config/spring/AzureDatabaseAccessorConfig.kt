package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.azure.*
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class AzureDatabaseAccessorConfig {

    @Autowired
    private lateinit var config: Config

    @Bean
    open  fun backOfficeDatabaseAccessor(): BackOfficeDatabaseAccessor {
        return AzureBackOfficeDatabaseAccessor(config.me.db.dictsConnString)
    }

    @Bean
    open  fun azureCashOperationsDatabaseAccessor( @Value("\${azure.cache.operation.table}") tableName: String)
            : CashOperationsDatabaseAccessor {
       return AzureCashOperationsDatabaseAccessor(config.me.db.balancesInfoConnString, tableName)
    }

    @Bean
    open  fun azureHistoryTicksDatabaseAccessor(@Value("\${application.settings.update.interval}") interval: Long)
            : HistoryTicksDatabaseAccessor {
       return AzureHistoryTicksDatabaseAccessor(config.me.db.hLiquidityConnString, interval)
    }

    @Bean
    open  fun azureLimitOrderDatabaseAccessor()
            : LimitOrderDatabaseAccessor {
       return AzureLimitOrderDatabaseAccessor(config.me.db.hLiquidityConnString)
    }

    @Bean
    open  fun azureMarketOrderDatabaseAccessor(@Value("\${azure.market.order.table}") tableName: String)
            : MarketOrderDatabaseAccessor {
       return AzureMarketOrderDatabaseAccessor(config.me.db.hTradesConnString, tableName)
    }


    @Bean
    open  fun azureReservedVolumesDatabaseAccessor(@Value("\${azure.reserved.volumes.table}") tableName: String)
            : ReservedVolumesDatabaseAccessor {
       return AzureReservedVolumesDatabaseAccessor(config.me.db.reservedVolumesConnString, tableName)
    }

    @Bean
    open  fun azureWalletDatabaseAccessor(@Value("\${azure.wallet.table}") tableName: String)
            : WalletDatabaseAccessor {
       return AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString, tableName)
    }

    @Bean
    open  fun AzureConfigDatabaseAccessor(@Value("\$azure.config.database.acessor.table") tableName: String)
            : ConfigDatabaseAccessor {
       return AzureConfigDatabaseAccessor(config.me.db.matchingEngineConnString, tableName)
    }


}