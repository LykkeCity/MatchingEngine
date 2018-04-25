package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.WalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.JedisPoolHolder

data class BalancesDatabaseAccessorsHolder(val primaryAccessor: WalletDatabaseAccessor,
                                           val secondaryAccessor: WalletDatabaseAccessor?,
                                           val jedisPoolHolder: JedisPoolHolder?)