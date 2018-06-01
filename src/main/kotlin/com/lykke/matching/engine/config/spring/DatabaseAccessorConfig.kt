package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.WalletDatabaseAccessorFactory
import com.lykke.matching.engine.database.WalletsStorage
import com.lykke.matching.engine.database.common.DefaultPersistenceManager
import com.lykke.matching.engine.database.redis.DefaultJedisHolder
import com.lykke.matching.engine.database.redis.EmptyJedisHolder
import com.lykke.matching.engine.database.redis.JedisHolder
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.RedisWalletDatabaseAccessor
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
    open fun jedisHolder(): JedisHolder {
        return when (config.me.walletsStorage) {
            WalletsStorage.Azure -> EmptyJedisHolder()
            WalletsStorage.Redis -> {
                val holder = balancesDatabaseAccessorsHolder()
                DefaultJedisHolder(holder.redisConfig)
            }
        }
    }

    @Bean
    open fun persistenceManager(): PersistenceManager {
        return when (config.me.walletsStorage) {
            WalletsStorage.Azure -> DefaultPersistenceManager(balancesDatabaseAccessorsHolder().primaryAccessor)
            WalletsStorage.Redis -> {
                val holder = balancesDatabaseAccessorsHolder()
                RedisPersistenceManager(
                        holder.primaryAccessor as RedisWalletDatabaseAccessor,
                        holder.secondaryAccessor,
                        jedisHolder() as DefaultJedisHolder
                )
            }
        }
    }
}