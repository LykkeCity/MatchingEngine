package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import redis.clients.jedis.Jedis

@Component
class BalancesDatabaseAccessorsHolderFactory: FactoryBean<BalancesDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var config: Config

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
                val jedis = Jedis(config.me.redis.host, config.me.redis.port, config.me.redis.timeout, config.me.redis.useSsl)
                jedis.connect()
                if (config.me.redis.password != null) {
                    jedis.auth(config.me.redis.password)
                }

                primaryAccessor = RedisWalletDatabaseAccessor(jedis, config.me.redis.balanceDatabase)

                secondaryAccessor = if (config.me.writeBalancesToSecondaryDb)
                    AzureWalletDatabaseAccessor(config.me.db.balancesInfoConnString, config.me.db.newAccountsTableName!!)
                else null
            }
        }
        return BalancesDatabaseAccessorsHolder(primaryAccessor, secondaryAccessor, config.me.redis)
    }
}