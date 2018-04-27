package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.MatchingEngineConfig
import redis.clients.jedis.Jedis

class WalletDatabaseAccessorFactory(private val config: MatchingEngineConfig) {

    fun createAccessorsHolder(): BalancesDatabaseAccessorsHolder {
        val primaryAccessor: WalletDatabaseAccessor
        val secondaryAccessor: WalletDatabaseAccessor?
        val jedis: Jedis?

        when (config.walletsStorage) {
            WalletsStorage.Azure -> {
                primaryAccessor = AzureWalletDatabaseAccessor(config.db.balancesInfoConnString,
                        config.db.accountsTableName ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME)

                secondaryAccessor = null
                jedis = null
            }

            WalletsStorage.Redis -> {
                jedis = Jedis(config.redis.balancesHost, config.redis.balancesPort)
                jedis.select(config.redis.balancesDbIndex)
                primaryAccessor = RedisWalletDatabaseAccessor(jedis)

                secondaryAccessor = if (config.writeBalancesToSecondaryDb)
                    AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, config.db.newAccountsTableName!!)
                else null
            }
        }
        return BalancesDatabaseAccessorsHolder(primaryAccessor, secondaryAccessor, jedis)
    }

}