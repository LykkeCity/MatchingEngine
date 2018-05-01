package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.MatchingEngineConfig
import redis.clients.jedis.JedisPool
import java.net.URI

class WalletDatabaseAccessorFactory(private val config: MatchingEngineConfig) {

    fun createAccessorsHolder(): BalancesDatabaseAccessorsHolder {
        val primaryAccessor: WalletDatabaseAccessor
        val secondaryAccessor: WalletDatabaseAccessor?
        val jedisPool: JedisPool?

        when (config.walletsStorage) {
            WalletsStorage.Azure -> {
                primaryAccessor = AzureWalletDatabaseAccessor(config.db.balancesInfoConnString,
                        config.db.accountsTableName ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME)

                secondaryAccessor = null
                jedisPool = null
            }

            WalletsStorage.Redis -> {
                jedisPool = JedisPool(URI(config.redis.balancesUri))
                primaryAccessor = RedisWalletDatabaseAccessor(jedisPool)

                secondaryAccessor = if (config.writeBalancesToSecondaryDb)
                    AzureWalletDatabaseAccessor(config.db.balancesInfoConnString, config.db.newAccountsTableName!!)
                else null
            }
        }
        return BalancesDatabaseAccessorsHolder(primaryAccessor, secondaryAccessor, jedisPool)
    }

}