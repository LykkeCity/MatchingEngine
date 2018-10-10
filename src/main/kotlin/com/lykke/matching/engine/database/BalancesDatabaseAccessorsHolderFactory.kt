package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*

@Component
class BalancesDatabaseAccessorsHolderFactory: FactoryBean<BalancesDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var redisWalletDatabaseAccessor: Optional<RedisWalletDatabaseAccessor>

    override fun getObjectType(): Class<*> {
        return BalancesDatabaseAccessorsHolder::class.java
    }

    override fun getObject(): BalancesDatabaseAccessorsHolder {
        val primaryAccessor: WalletDatabaseAccessor
        val secondaryAccessor: WalletDatabaseAccessor?

        when (config.me.storage) {
            Storage.Azure -> {
                primaryAccessor = AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString,
                        config.me.db.accountsTableName ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME)

                secondaryAccessor = null
            }
            Storage.Redis -> {
                primaryAccessor = redisWalletDatabaseAccessor.get()

                secondaryAccessor = if (config.me.writeBalancesToSecondaryDb)
                    AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString, config.me.db.newAccountsTableName!!)
                else null
            }
        }
        return BalancesDatabaseAccessorsHolder(primaryAccessor, secondaryAccessor)
    }
}