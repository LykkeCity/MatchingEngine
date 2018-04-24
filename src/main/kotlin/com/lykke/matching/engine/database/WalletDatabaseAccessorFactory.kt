package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.JedisPoolFactory
import com.lykke.matching.engine.database.redis.JedisPoolHolder
import com.lykke.matching.engine.database.redis.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.MatchingEngineConfig

class WalletDatabaseAccessorFactory(private val config: MatchingEngineConfig) {

    fun createAccessorsHolder(): BalancesDatabaseAccessorsHolder {
        val primaryAccessor: WalletDatabaseAccessor
        val secondaryAccessor: WalletDatabaseAccessor?
        val jedisPoolHolder: JedisPoolHolder?

        when (config.walletsStorage) {
            WalletsStorage.Azure -> {
                primaryAccessor = AzureWalletDatabaseAccessor(config.db.balancesInfoConnString,
                        config.db.accountsTableName ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME)

                secondaryAccessor = null
                jedisPoolHolder = null
            }

            WalletsStorage.Redis -> {
                jedisPoolHolder = JedisPoolFactory()
                        .create(config.redis.balancesHost, config.redis.balancesPort, config.redis.balancesDbIndex)
                primaryAccessor = RedisWalletDatabaseAccessor(jedisPoolHolder)

                secondaryAccessor = if (config.writeBalancesToSecondaryDb)
                    AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, config.db.newAccountsTableName!!)
                else null
            }
        }
        return BalancesDatabaseAccessorsHolder(primaryAccessor, secondaryAccessor, jedisPoolHolder)
    }

}