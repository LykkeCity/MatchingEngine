package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.WalletDatabaseAccessor
import redis.clients.jedis.JedisPool

data class BalancesDatabaseAccessorsHolder(val primaryAccessor: WalletDatabaseAccessor,
                                           val secondaryAccessor: WalletDatabaseAccessor?,
                                           val jedisPool: JedisPool?)