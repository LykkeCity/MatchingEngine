package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.WalletDatabaseAccessor
import redis.clients.jedis.Jedis

data class BalancesDatabaseAccessorsHolder(val primaryAccessor: WalletDatabaseAccessor,
                                           val secondaryAccessor: WalletDatabaseAccessor?,
                                           val jedis: Jedis?)